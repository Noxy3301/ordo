#ifndef HELIOS_STORAGE_SRC_INDEX_IMPL_MASSTREE_INDEX_H
#define HELIOS_STORAGE_SRC_INDEX_IMPL_MASSTREE_INDEX_H

#include <storage/config.h>
#include <storage/pax_store.h>

#include <functional>
#include <memory>
#include <optional>
#include <string_view>

#include "types/data_item.h"
#include "util/epoch_framework.h"

namespace helios::storage {
namespace index {

// PImpl wrapper around masstree-beta. Masstree headers are confined to
// masstree_index.cc; this header stays free of masstree to avoid leaking
// its templates / macros into the rest of LDB (and through there, into tests
// that do not have masstree on their include path).
class MasstreeIndex final {
 public:
  MasstreeIndex(Config c, epoch::Framework &e);
  ~MasstreeIndex();

  /**
   * @brief Routes future blank primary rows through a table PAX store.
   *
   * @details Secondary indexes never set a PaxStore: they store index
   * metadata rather than table row payloads.
   */
  void SetPaxStore(pax::PaxStore *store);

  DataItem *Get(std::string_view key);
  bool Put(std::string_view key, DataItem &&rhs);

  // Seed a blank entry for a key that later writes fill in. Idempotent on an
  // existing key.
  void PutBlank(std::string_view key);

  // Range operations. Return the number of keys the walk emitted.
  size_t Scan(std::string_view begin, std::optional<std::string_view> end,
              std::function<bool(std::string_view)> operation);
  size_t Scan(std::string_view begin, std::string_view end,
              std::function<bool(std::string_view, DataItem &)> operation);
  size_t ScanReverse(std::string_view begin,
                     std::optional<std::string_view> end,
                     std::function<bool(std::string_view)> operation);
  size_t ScanReverse(
      std::string_view begin, std::string_view end,
      std::function<bool(std::string_view, DataItem &)> operation);

  void ForEach(std::function<bool(std::string_view, DataItem &)> operation);

  // Structurally remove a committed tombstone. Called by the deferred purge
  // reaper only, after it has locked `expected`, verified the delete TID, and
  // confirmed the key still resolves to the same DataItem. `retired_tid` is
  // published on the removed item before it is RCU-retired.
  bool Purge(std::string_view key, DataItem *expected,
             TransactionId retired_tid = {});

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

// Hooks into masstree-beta's RCU machinery. masstree's globalepoch needs
// to be driven by the host's epoch ticker (MasstreeAdvanceEpoch). Threads
// that touch the tree enrol implicitly via masstree ops; they close their
// critical section by calling MasstreeReleaseThreadEpoch at a safe
// boundary (no raw DataItem* / leaf pointer from this section can be used
// past the release). There is intentionally no "advance without release"
// API — re-stamping gc_epoch_ mid-section would let RCU reclaim pointers
// the caller is still using.
// Both per-thread functions are no-ops when no masstree threadinfo has
// been initialised on the current thread.
void MasstreeAdvanceEpoch();
void MasstreeReleaseThreadEpoch();
// Like MasstreeReleaseThreadEpoch but pessimistically advances the global
// epoch in a loop so the calling thread's limbo gets fully drained before
// it exits. Heavier than a regular release; intended for connection-close
// paths only.
void MasstreeFullyDrainThread();

}  // namespace index
}  // namespace helios::storage

#endif  // HELIOS_STORAGE_SRC_INDEX_IMPL_MASSTREE_INDEX_H
