#include "index/masstree_index.h"

#include <atomic>
#include <cstring>
#include <mutex>
#include <utility>

#include "util/debug_sync.h"

// Masstree headers. Must come after the PImpl header guard so other LDB
// sources never see them; masstree's include path is PRIVATE to LDB and
// therefore unreachable from public headers and tests. config.h defines the
// macros compiler.hh reads, so this block is not sortable.
// clang-format off
#include "config.h"
#include "compiler.hh"
#include "kvthread.hh"
#include "masstree.hh"
#include "masstree_insert.hh"
#include "masstree_remove.hh"
#include "masstree_scan.hh"
#include "masstree_tcursor.hh"
#include "string.hh"
// clang-format on

// Globals required by masstree-beta. masstree's kvthread.cc references these
// as externs; exactly one translation unit must define them. Types must
// match kvthread.hh:33-35.
//
// Intentionally left at their initial values: this wrapper uses masstree's
// B+tree structure and nodeversion-based locking for concurrency, but does
// NOT drive masstree's internal RCU machinery. Matches published Silo+
// masstree practice — CCBench's masstree_wrapper.hh and Tu Silo's
// simple_threadinfo stub take the same approach — and has no semantic
// consequences: masstree's insert/scan/remove correctness relies on
// nodeversion bits, not these epochs. Tradeoff: every masstree allocation
// that would otherwise be reclaimed via RCU (overwrite-replaced
// DataItem*, retired leaves/internodes/ksuffix blocks from internal
// splits), plus the entire live tree at process exit (destroy() is
// deliberately not called — see ~Impl()), leaks for the process lifetime.
// Bounded by bench-scope insert churn; not suitable for long-running
// service deployment without a real reclamation path.
relaxed_atomic<mrcu_epoch_type> globalepoch{1};
relaxed_atomic<mrcu_epoch_type> active_epoch{1};
volatile bool recovering = false;

namespace helios::storage {
namespace index {

namespace {

class key_unparse_unsigned {
 public:
  static int unparse_key(Masstree::key<std::uint64_t> key, char *buf,
                         int buflen) {
    return snprintf(buf, buflen, "%" PRIu64, key.ikey());
  }
};

struct table_params : public Masstree::nodeparams<15, 15> {
  using value_type = DataItem *;
  using value_print_type = Masstree::value_print<value_type>;
  using threadinfo_type = threadinfo;
  using key_unparse_type = key_unparse_unsigned;
  static constexpr ssize_t print_max_indent_depth = 12;
};

using table_type = Masstree::basic_table<table_params>;
using unlocked_cursor_type = Masstree::unlocked_tcursor<table_params>;
using cursor_type = Masstree::tcursor<table_params>;

thread_local threadinfo *tls_ti = nullptr;
// True iff this thread has called rcu_start since its last rcu_stop. Used to
// make ensure_thread_active idempotent within a critical section: re-stamping
// gc_epoch_ on every masstree op would advance the thread past the epoch
// protecting raw DataItem* pointers the caller is still holding (e.g.
// Silo's validation_set_ holds item_p_cache across reads, and a concurrent
// committed delete schedules those DataItems for RCU free). With the gate,
// a thread's gc_epoch_ stays at the epoch of its FIRST masstree access until
// an explicit release at a safe boundary.
thread_local bool tls_enrolled = false;
std::atomic<int> next_thread_id{0};
std::mutex thread_init_mutex;

// threadinfo::make() prepends to masstree's global allthreads list without
// synchronization (kvthread.cc:57-58). Serialize first-use-per-thread to
// avoid UB when multiple worker threads register concurrently. Contended
// only on the first op per thread. Does NOT enrol the thread in the
// current RCU epoch — that is ensure_thread_active's job. Splitting
// registration from enrolment lets short-lived callers (the Database
// construction thread, server-side threads between operations) leave
// gc_epoch_ at 0 so they do not pin min_active_epoch().
inline void ensure_thread_init() {
  if (__builtin_expect(tls_ti == nullptr, 0)) {
    std::lock_guard<std::mutex> lg(thread_init_mutex);
    if (tls_ti == nullptr) {
      tls_ti = threadinfo::make(
          threadinfo::TI_PROCESS,
          next_thread_id.fetch_add(1, std::memory_order_relaxed));
    }
  }
}

// Ensure threadinfo exists AND this thread is enrolled at the current
// masstree epoch — but only if the thread is not already enrolled. This
// idempotency is correctness-critical: every public op uses this, and
// re-stamping gc_epoch_ in the middle of a transaction would let RCU
// reclaim DataItem* pointers the caller is still holding (e.g. Silo
// validation_set_ entries captured from an earlier read). The thread stays
// enrolled at its first-op epoch until an explicit release at a safe
// boundary (MasstreeReleaseThreadEpoch).
inline void ensure_thread_active() {
  ensure_thread_init();
  if (!tls_enrolled) {
    tls_ti->rcu_start();
    tls_enrolled = true;
  }
}

// RCU callback for deferred DataItem deletion. Allocated through
// threadinfo::allocate so it lives in masstree's accounting pool, and
// frees itself in operator() after deleting the wrapped DataItem (the
// same shape as masstree's own gc_layer_rcu_callback, see
// masstree_remove.hh:135-146).
struct RcuFreeCallback : public threadinfo::mrcu_callback {
  DataItem *item;
  explicit RcuFreeCallback(DataItem *it) : item(it) {}
  void operator()(threadinfo &ti) override {
    delete item;
    ti.deallocate(this, sizeof(RcuFreeCallback), memtag_masstree_gc);
  }
};

inline void RcuFree(DataItem *item) {
  if (item == nullptr) return;
  void *mem = tls_ti->allocate(sizeof(RcuFreeCallback), memtag_masstree_gc);
  auto *cb = new (mem) RcuFreeCallback(item);
  tls_ti->rcu_register(cb);
}

// Adapter that drives masstree's forward/reverse scan into the LDB callback
// shape (operation returning `true` means cancel).
struct ScanAdapter {
  const char *end_ptr;
  size_t end_len;
  bool has_end;
  std::function<bool(std::string_view)> cb;
  size_t count = 0;

  template <typename SS, typename K>
  void visit_leaf(const SS &, const K &, threadinfo &) {}

  // Returns true to keep scanning, false to stop (masstree convention).
  bool visit_value(Masstree::Str key, DataItem * /*val*/, threadinfo &) {
    if (has_end) {
      const int cmp = std::memcmp(
          end_ptr, key.s, std::min(end_len, static_cast<size_t>(key.len)));
      const bool end_greater =
          cmp > 0 || (cmp == 0 && end_len > static_cast<size_t>(key.len));
      if (!end_greater) return false;  // key >= end -> out of range, stop
    }
    ++count;
    if (cb(std::string_view(key.s, key.len))) return false;  // caller cancel
    return true;
  }
};

struct ScanValueAdapter {
  const char *end_ptr;
  size_t end_len;
  bool has_end;
  std::function<bool(std::string_view, DataItem &)> cb;
  size_t count = 0;

  template <typename SS, typename K>
  void visit_leaf(const SS &, const K &, threadinfo &) {}

  bool visit_value(Masstree::Str key, DataItem *val, threadinfo &) {
    if (has_end) {
      const int cmp = std::memcmp(
          end_ptr, key.s, std::min(end_len, static_cast<size_t>(key.len)));
      const bool end_greater =
          cmp > 0 || (cmp == 0 && end_len > static_cast<size_t>(key.len));
      if (!end_greater) return false;
    }
    ++count;
    if (cb(std::string_view(key.s, key.len), *val)) return false;
    return true;
  }
};

using leaf_type = Masstree::leaf<table_params>;

}  // namespace

struct MasstreeIndex::Impl {
  table_type table_;

  Impl() {
    // Enrol briefly so table_.initialize can allocate the root node through
    // the threadinfo's RCU-aware allocator, then leave the epoch so the
    // construction thread does not pin min_active_epoch at the initial
    // value forever. If the same thread later does masstree ops, each op
    // path goes through ensure_thread_active and re-enrols.
    //
    // A caller already inside an RCU critical section keeps its enrolment:
    // rcu_stop() would clear gc_epoch_ while raw DataItem* pointers it still
    // holds could be scheduled for RCU free by another thread's physical
    // delete. The existing rcu_start covers the initialize.
    ensure_thread_init();
    const bool was_enrolled = tls_enrolled;
    if (!was_enrolled) {
      tls_ti->rcu_start();
    }
    table_.initialize(*tls_ti);
    if (!was_enrolled) {
      tls_ti->rcu_stop();
    }
  }

  ~Impl() {
    // Do not call table_.destroy(): it schedules RCU free callbacks via
    // deallocate_rcu, but this wrapper never advances the RCU epoch so the
    // callbacks would never run. Consequence: at process exit, the entire
    // live tree (every leaf/internode allocation) and every stored
    // DataItem* leaks. Accepted for benchmark-scope runs only; a real
    // reclamation path is required for long-running service deployment.
  }

  DataItem *Get(std::string_view key) {
    ensure_thread_active();
    unlocked_cursor_type lp(table_, key.data(), key.size());
    if (lp.find_unlocked(*tls_ti)) return lp.value();
    return nullptr;
  }

  // Blank rows created after SetPaxStore are initialized in PAX mode so their
  // first committed payload scatters into the table's strips.
  pax::PaxStore *pax_store_ = nullptr;
  DataItem *NewBlankItem() {
    auto *item = new DataItem();
    if (pax_store_ != nullptr) item->buffer.InitPaxBlank(pax_store_);
    return item;
  }

  // Upsert with a freshly-allocated DataItem. Returns true on success.
  bool Put(std::string_view key, DataItem &&rhs) {
    ensure_thread_active();
    auto *fresh = new DataItem(std::move(rhs));
    cursor_type lp(table_, key.data(), key.size());
    bool found = lp.find_insert(*tls_ti);
    if (found) {
      // Overwrite existing slot. Previous DataItem* is leaked: a concurrent
      // reader may still hold the raw pointer returned by Get(). Phase 2b
      // accepts the leak; a Phase 3/4 step will hook this into masstree's
      // RCU limbo list.
      lp.value() = fresh;
    } else {
      lp.value() = fresh;
    }
    fence();
    // 1 == structural insert (bumps the leaf's vinsert counter), 0 == in-place
    // overwrite. Claiming an insert on overwrite would falsely trigger phantom
    // retries on concurrent scanners watching this leaf.
    lp.finish(found ? 0 : 1, *tls_ti);
    return true;
  }

  // Structural removal of a committed delete. Invoked by the deferred reaper
  // after it has CAS-locked the DataItem and verified the tombstone TID.
  // Returns true if the key was physically removed; false if the slot is
  // already gone or a racing replacement won the position.
  bool Purge(std::string_view key, DataItem *expected,
             TransactionId retired_tid) {
    ensure_thread_active();
    cursor_type lp(table_, key.data(), key.size());
    const bool found = lp.find_locked(*tls_ti);
    if (!found) {
      lp.finish(0, *tls_ti);
      return false;
    }
    DataItem *current = lp.value();
    if (expected != nullptr && current != expected) {
      // Some other writer replaced the slot between our commit-time decision
      // and this erase. Leave the new occupant alone.
      lp.finish(0, *tls_ti);
      return false;
    }
    // A reader that resolved this entry parks on its lock bit until the
    // retired TID is published below.
    HELIOS_DEBUG_SYNC("reaper.purge_locked_window");
    // finish(-1) calls finish_remove which removes the permutation slot and
    // RCU-frees the leaf if it becomes empty. The DataItem* itself rides on
    // a separate RCU callback below.
    lp.finish(-1, *tls_ti);
    if (!retired_tid.IsEmpty()) {
      current->transaction_id.store(retired_tid);
    }
    RcuFree(current);
    return true;
  }

  // Idempotent blank insert: never removes, just ensures a slot exists.
  void PutBlank(std::string_view key) {
    ensure_thread_active();
    cursor_type lp(table_, key.data(), key.size());
    bool found = lp.find_insert(*tls_ti);
    if (!found) {
      lp.value() = NewBlankItem();
    }
    fence();
    lp.finish(found ? 0 : 1, *tls_ti);
  }

  size_t Scan(std::string_view begin, std::optional<std::string_view> end,
              std::function<bool(std::string_view)> op) {
    ensure_thread_active();
    ScanAdapter adapter{end.has_value() ? end->data() : nullptr,
                        end.has_value() ? end->size() : 0, end.has_value(),
                        std::move(op), 0};
    Masstree::Str firstkey(begin.data(), begin.size());
    table_.scan(firstkey, /*emit_firstkey=*/true, adapter, *tls_ti);
    return adapter.count;
  }

  size_t Scan(std::string_view begin, std::string_view end,
              std::function<bool(std::string_view, DataItem &)> op) {
    ensure_thread_active();
    ScanValueAdapter adapter{end.data(), end.size(), true, std::move(op), 0};
    Masstree::Str firstkey(begin.data(), begin.size());
    table_.scan(firstkey, /*emit_firstkey=*/true, adapter, *tls_ti);
    return adapter.count;
  }

  size_t ScanReverse(std::string_view begin,
                     std::optional<std::string_view> end,
                     std::function<bool(std::string_view)> op) {
    ensure_thread_active();
    // Reverse scan walks downward from `end - 1`, stopping once key < begin.
    // Range is [begin, end) just like forward Scan.
    auto adapter_op = [b_ptr = begin.data(), b_len = begin.size(),
                       cb = std::move(op)](std::string_view key) mutable {
      const int cmp =
          std::memcmp(b_ptr, key.data(), std::min(b_len, key.size()));
      const bool key_below_begin = cmp > 0 || (cmp == 0 && b_len > key.size());
      if (key_below_begin) return true;  // below begin -> stop
      return cb(key);
    };
    ScanAdapter adapter{nullptr, 0, false, std::move(adapter_op), 0};
    if (end.has_value()) {
      Masstree::Str firstkey(end->data(), end->size());
      table_.rscan(firstkey, /*emit_firstkey=*/false, adapter, *tls_ti);
    } else {
      // rscan from infinity: use an empty firstkey with emit=true to
      // walk the tail backwards.
      table_.rscan(Masstree::Str(), /*emit_firstkey=*/true, adapter, *tls_ti);
    }
    return adapter.count;
  }

  size_t ScanReverse(std::string_view begin, std::string_view end,
                     std::function<bool(std::string_view, DataItem &)> op) {
    ensure_thread_active();
    auto adapter_op = [b_ptr = begin.data(), b_len = begin.size(),
                       cb = std::move(op)](std::string_view key,
                                           DataItem &val) mutable {
      const int cmp =
          std::memcmp(b_ptr, key.data(), std::min(b_len, key.size()));
      const bool key_below_begin = cmp > 0 || (cmp == 0 && b_len > key.size());
      if (key_below_begin) return true;
      return cb(key, val);
    };
    ScanValueAdapter adapter{nullptr, 0, false, std::move(adapter_op), 0};
    Masstree::Str firstkey(end.data(), end.size());
    table_.rscan(firstkey, /*emit_firstkey=*/false, adapter, *tls_ti);
    return adapter.count;
  }

  void ForEach(std::function<bool(std::string_view, DataItem &)> op) {
    ensure_thread_active();
    ScanValueAdapter adapter{nullptr, 0, false, std::move(op), 0};
    table_.scan(Masstree::Str(), /*emit_firstkey=*/true, adapter, *tls_ti);
  }
};

MasstreeIndex::MasstreeIndex(Config /*c*/, epoch::Framework & /*e*/)
    : impl_(std::make_unique<Impl>()) {}

MasstreeIndex::~MasstreeIndex() = default;

void MasstreeIndex::SetPaxStore(pax::PaxStore *store) {
  impl_->pax_store_ = store;
}

DataItem *MasstreeIndex::Get(std::string_view key) { return impl_->Get(key); }

bool MasstreeIndex::Put(std::string_view key, DataItem &&rhs) {
  return impl_->Put(key, std::move(rhs));
}

void MasstreeIndex::PutBlank(std::string_view key) { impl_->PutBlank(key); }

size_t MasstreeIndex::Scan(std::string_view begin,
                           std::optional<std::string_view> end,
                           std::function<bool(std::string_view)> operation) {
  return impl_->Scan(begin, end, std::move(operation));
}

size_t MasstreeIndex::Scan(
    std::string_view begin, std::string_view end,
    std::function<bool(std::string_view, DataItem &)> operation) {
  return impl_->Scan(begin, end, std::move(operation));
}

size_t MasstreeIndex::ScanReverse(
    std::string_view begin, std::optional<std::string_view> end,
    std::function<bool(std::string_view)> operation) {
  return impl_->ScanReverse(begin, end, std::move(operation));
}

size_t MasstreeIndex::ScanReverse(
    std::string_view begin, std::string_view end,
    std::function<bool(std::string_view, DataItem &)> operation) {
  return impl_->ScanReverse(begin, end, std::move(operation));
}

void MasstreeIndex::ForEach(
    std::function<bool(std::string_view, DataItem &)> operation) {
  impl_->ForEach(std::move(operation));
}

bool MasstreeIndex::Purge(std::string_view key, DataItem *expected,
                          TransactionId retired_tid) {
  return impl_->Purge(key, expected, retired_tid);
}

void MasstreeAdvanceEpoch() {
  // Bump masstree's global epoch and recompute the reclamation watermark.
  // Mirrors mttest.cc's main-thread tick (mttest.cc:124-131). The mutex
  // serialises with ensure_thread_init's threadinfo::make, which prepends
  // to the unsynchronised threadinfo::allthreads list that min_active_epoch
  // walks (kvthread.cc:53-58, kvthread.hh:368-377).
  std::lock_guard<std::mutex> lg(thread_init_mutex);
  globalepoch.store(globalepoch.load() + 1);
  active_epoch.store(threadinfo::min_active_epoch());
}

void MasstreeReleaseThreadEpoch() {
  // End this thread's RCU critical section: drain whatever is eligible
  // under the current active_epoch and clear gc_epoch_. Callers MUST
  // guarantee no raw DataItem* or masstree leaf pointer from inside this
  // section survives past the release — that's the whole point of marking
  // the section closed.
  //
  // This is the hot-path release (called at every RPC safe boundary). It
  // does ONE rcu_stop pass: cheap when limbo is small, and items that
  // remain on the local limbo are drained on the thread's next release
  // when active_epoch has moved further. For a thread that is about to
  // exit and will never call release again, use MasstreeFullyDrainThread
  // instead.
  if (tls_ti == nullptr) return;
  tls_ti->rcu_stop();
  tls_enrolled = false;
}

void MasstreeFullyDrainThread() {
  // Best-effort drain of the calling thread's local limbo before it
  // exits. Strategy: leave the min_active_epoch participant set first
  // (rcu_stop sets gc_epoch_=0 and frees the first eligible batch), then
  // bump the global epoch and re-call rcu_stop to peel further 128-entry
  // batches.
  //
  // KNOWN LIMITATION (cross-thread limbo leak): this is best-effort, not
  // a guarantee. Two conditions can permanently strand entries on this
  // thread's local limbo after it exits:
  //
  //   (a) Some OTHER thread holds an old gc_epoch_ that pins active_epoch
  //       below the epoch our items were retired at. Our items remain
  //       ineligible; the loop bumps globalepoch but min_active_epoch
  //       stays low.
  //   (b) We retired more entries than 4096 * 128 = 512K within this
  //       section. The loop cap exits before they are all drained.
  //
  // After the detached connection thread exits, its threadinfo lingers
  // on masstree's allthreads list but no future call runs on it. The
  // limbo is per-threadinfo and only the owning thread can safely drain
  // it via masstree's API, so the stranded items leak for the process
  // lifetime.
  //
  // Closing this properly requires either (i) a server architecture
  // change to keep RPC threads alive so the same thread eventually drains
  // its own limbo on a later op, or (ii) patching masstree-beta to expose a
  // cross-thread drain primitive the epoch ticker could use on exited
  // threadinfos.
  //
  // In the meantime: the leak is bounded by per-connection-close
  // workload. Benchmarks that keep connections alive (BenchBase / sysbench
  // style pools) do not trigger it. Long-running services with high
  // connection churn under contended workloads will accumulate it.
  //
  // Not for the hot path: each MasstreeAdvanceEpoch acquires
  // thread_init_mutex and re-walks the allthreads list, so calling this
  // on every RPC boundary causes contention collapse at high
  // concurrency.
  if (tls_ti == nullptr) return;
  tls_ti->rcu_stop();
  tls_enrolled = false;
  for (int i = 0; i < 4096; ++i) {
    MasstreeAdvanceEpoch();
    tls_ti->rcu_stop();
  }
}

}  // namespace index
}  // namespace helios::storage
