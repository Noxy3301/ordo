/*
 *   Copyright (C) 2020 Nippon Telegraph and Telephone Corporation.

 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at

 *   http://www.apache.org/licenses/LICENSE-2.0

 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#ifndef LINEAIRDB_SILO_H
#define LINEAIRDB_SILO_H

#include <lineairdb/tx_status.h>

#include <atomic>
#include <cstddef>
#include <cstring>
#include <xmmintrin.h>
#include <vector>

#include "concurrency_control/concurrency_control_base.h"
#include "pax/version_store.hpp"
#include "types/data_item.hpp"
#include "types/definitions.h"
#include "util/debug_sync.hpp"

namespace LineairDB {

namespace ConcurrencyControl {

class Silo final : public ConcurrencyControlBase {
 private:
  struct ValidationItem {
    const DataItem* item_p_cache;
    TransactionId transaction_id;
  };

  std::vector<ValidationItem> validation_set_;

 public:
  Silo(TransactionReferences&& tx)
      : ConcurrencyControlBase(std::forward<TransactionReferences&&>(tx)){};
  ~Silo() final override{};

  // TransactionReferences has reference members, so normal assignment (=) is
  // not possible. Destroy-then-placement-new rebinds the references in place.
  void Reset(TransactionReferences&& new_ref) override {
    tx_ref_.~TransactionReferences();
    new (&tx_ref_) TransactionReferences(std::move(new_ref));
    validation_set_.clear();
    aborted_by_duplicate_key_ = false;
    pre_commit_validator_ = {};
  }

  const DataItem Read(const std::string_view,
                      DataItem* index_leaf) final override {
    assert(index_leaf != nullptr);

    DataItem snapshot;
    for (;;) {
      auto tx_id = index_leaf->transaction_id.load();

      if (tx_id.tid & 1u) {  // locked
        _mm_pause();
        continue;
      }

      snapshot = *index_leaf;

      if (index_leaf->transaction_id.load() == tx_id) {
        validation_set_.push_back({index_leaf, tx_id});
        return snapshot;
      }
    }
  };

  /**
   * @brief Copy a DataItem using Silo's double-TID stable-read loop.
   *
   * @details Unlike Read(), this does not append to validation_set_. It is only
   * for non-unique secondary-index write seeds whose commit result is later
   * installed by merging recorded Add/Remove deltas under the write lock.
   */
  const DataItem ReadUnvalidated(const std::string_view,
                                 DataItem* index_leaf) final override {
    assert(index_leaf != nullptr);

    DataItem snapshot;
    for (;;) {
      auto tx_id = index_leaf->transaction_id.load();

      if (tx_id.tid & 1u) {
        _mm_pause();
        continue;
      }

      snapshot = *index_leaf;

      if (index_leaf->transaction_id.load() == tx_id) {
        return snapshot;
      }
    }
  };

  // See concurrency_control_base.h for why ReadDirect exists (Scan perf).
  // TID double-check protocol ensures the returned pointer is consistent.
  //
  // FIXME: ReadDirect is a Scan-specific lightweight path that registers
  // into validation_set_ without creating a Snapshot in read_set_. This
  // works for research purposes but breaks LineairDB's CC-switchable
  // design, as validation_set_ is Silo-specific internal state.
  //
  // The root issue is Snapshot cost: 384B + heap-copied row data per entry.
  // Under scan-heavy workloads, this doesn't scale: memory allocation cost
  // grows with client count and scan size, eventually causing OOM.
  //
  // Possible directions:
  // - Scan-mode Snapshot that skips data_item_copy (validation-only)
  // - Thread-local pooled allocator for Snapshot / DataBuffer
  // - Decouple validation tracking from read_set_ at the CC interface level
  std::pair<const std::byte*, size_t> ReadDirect(
      const std::string_view, DataItem* index_leaf,
      TransactionId& out_tid) final override {
    assert(index_leaf != nullptr);

    for (;;) {
      // Step 1: Load TID. Writers set the lock bit (LSB) before modifying data.
      auto tx_id = index_leaf->transaction_id.load();

      // Step 2: If lock bit is set, a writer holds this record. Spin until released.
      if (tx_id.tid & 1u) {
        _mm_pause();
        continue;
      }

      // Step 3: Read value pointer + size from DataItem buffer (no memcpy).
      // Primary rows never use the secondary posting-list pointer, so keep
      // this hot path to the size-only liveness check.
      const bool live = index_leaf->IsPrimaryInitialized();
      const std::byte* val;
      size_t sz;
      if (index_leaf->buffer.is_pax()) {
        // PAX rows have no contiguous bytes: gather into a thread-local
        // scratch. The callback consumes the pointer before the next
        // ReadDirect on this thread; a torn gather is rejected by the TID
        // re-check in Step 4 (same discipline as the zero-copy pointer).
        thread_local std::vector<std::byte> pax_scratch;
        sz = index_leaf->buffer.size;
        if (live && sz > 0) {
          if (pax_scratch.size() < sz) pax_scratch.resize(sz);
          index_leaf->buffer.GatherInto(pax_scratch.data());
          val = pax_scratch.data();
        } else {
          val = nullptr;
          sz = 0;
        }
      } else {
        val = index_leaf->buffer.value;
        sz = index_leaf->buffer.size;
      }

      // Step 4: Re-check TID. If unchanged, no concurrent writer modified the
      // data between Step 1 and Step 3, so the pointer is safe to return.
      if (index_leaf->transaction_id.load() == tx_id) {
        validation_set_.push_back({index_leaf, tx_id});
        out_tid = tx_id;
        return live ? std::pair<const std::byte*, size_t>{val, sz}
                    : std::pair<const std::byte*, size_t>{nullptr, 0};
      }
      // TID changed → a writer intervened. Retry from Step 1.
    }
  };

  void Write(const std::string_view, const std::byte* const, const size_t,
             DataItem*) final override{};
  void Abort() final override{};
  bool Precommit() final override {
    /** Sorting write set to prevent deadlock **/
    std::sort(tx_ref_.write_set_ref_.begin(), tx_ref_.write_set_ref_.end(),
              Snapshot::Compare);

    /** Acquire Lock **/
    std::vector<DataItem*> locked_items;
    locked_items.reserve(tx_ref_.write_set_ref_.size());
    auto unlock_locked_items = [&]() {
      for (auto* locked_item : locked_items) {
        auto current = locked_item->transaction_id.load();
        if (current.tid & 1u) {
          current.tid--;
          locked_item->transaction_id.store(current);
        }
      }
    };

    for (auto& snapshot : tx_ref_.write_set_ref_) {
      auto* item = snapshot.index_cache;
      assert(item != nullptr);
      __builtin_prefetch(item, 1, 3);

      for (;;) {
        auto current = item->transaction_id.load();
        if (current.tid & 1llu) {
          _mm_pause();
          continue;
        }
        auto desired = current;
        desired.tid |= 1llu;
        bool lock_acquired =
            item->transaction_id.compare_exchange_weak(current, desired);
        if (lock_acquired) {
          locked_items.push_back(item);
          snapshot.data_item_copy.transaction_id.store(desired);
          DataItem* attached_item = nullptr;
          bool checked_attachment = false;
          if (snapshot.index_name.empty() && snapshot.pi_ref != nullptr) {
            attached_item = snapshot.pi_ref->Get(snapshot.key);
            checked_attachment = true;
          } else if (!snapshot.index_name.empty() &&
                     snapshot.si_ref != nullptr) {
            attached_item = snapshot.si_ref->Get(snapshot.key);
            checked_attachment = true;
          }
          if (checked_attachment && attached_item != item) {
            unlock_locked_items();
            return false;
          }
          // Validate own-locked reads against the read-time TID with our lock
          // bit set. ReadDirect can duplicate entries, so update all matches.
          for (auto& read_item : validation_set_) {
            if (read_item.item_p_cache == item) {
              read_item.transaction_id.tid |= 1llu;
            }
          }
          break;
        }
      }
    }

    // CompilerFence();
    tx_ref_.epoch_framework_ref_.MakeMeOffline();
    tx_ref_.epoch_framework_ref_.MakeMeOnline();
    // CompilerFence();

    /** Validation Phase **/
    if (!AntiDependencyValidation()) {
      // if validation failed, unlock all objects
      for (auto& snapshot : tx_ref_.write_set_ref_) {
        auto current = snapshot.index_cache->transaction_id.load();
        current.tid--;
        snapshot.index_cache->transaction_id.store(current);
      }
      return false;
    }

    // Deferred phantom validation (Masstree). Runs under write locks and
    // after AntiDepValidation, so concurrent leaf-version drift happening-
    // before this tx's serial point is observed. PL leaves the validator
    // unset; Masstree installs one in Transaction::Impl::Precommit.
    if (pre_commit_validator_ && !pre_commit_validator_()) {
      for (auto& snapshot : tx_ref_.write_set_ref_) {
        auto current = snapshot.index_cache->transaction_id.load();
        current.tid--;
        snapshot.index_cache->transaction_id.store(current);
      }
      return false;
    }

    // A write that claimed a free key must still find it free. The entry is
    // the one the write resolved and the lock loop proved is attached to the
    // key, so no reinsert of the slot can hide a row from this check.
    for (const auto& snapshot : tx_ref_.write_set_ref_) {
      if (!snapshot.is_insert) continue;
      // Only primary writes may carry the flag: SI liveness is not
      // IsPrimaryInitialized().
      assert(snapshot.index_name.empty());
      if (!snapshot.index_cache->IsPrimaryInitialized()) continue;
      aborted_by_duplicate_key_ = true;
      for (auto& locked : tx_ref_.write_set_ref_) {
        auto current = locked.index_cache->transaction_id.load();
        current.tid--;
        locked.index_cache->transaction_id.store(current);
      }
      return false;
    }

    /** Buffer Update **/
    //
    // Deletes install tombstones. Physical removal is deferred to the
    // epoch reaper so same-key reinserts reuse the slot and preserve the
    // slot's monotonic TID chain.
    //
    // Tags the install region with the commit epoch so the PAX
    // before-image capture can label its entries.
    Pax::ScopedCommitEpoch commit_epoch_scope(
        tx_ref_.epoch_framework_ref_.GetMyThreadLocalEpoch());
    size_t installed = 0;
    for (auto& snapshot : tx_ref_.write_set_ref_) {
      if (installed > 0) {
        LINEAIRDB_DEBUG_SYNC("silo_commit.between_row_installs");
      }
      ++installed;
      if (!snapshot.index_name.empty() &&
          !snapshot.secondary_index_deltas.empty() &&
          !snapshot.index_type.IsUnique()) {
        // Non-unique SI writes commute as Add/Remove deltas. Apply them to
        // the locked current PK-list instead of overwriting with a stale copy.
        for (const auto& delta : snapshot.secondary_index_deltas) {
          const auto* primary_key =
              reinterpret_cast<const std::byte*>(delta.primary_key.data());
          switch (delta.op) {
            case SecondaryIndexOp::Add:
              snapshot.index_cache->AddSecondaryIndexValue(
                  primary_key, delta.primary_key.size());
              break;
            case SecondaryIndexOp::Remove:
              snapshot.index_cache->RemoveSecondaryIndexValue(
                  primary_key, delta.primary_key.size());
              break;
            case SecondaryIndexOp::None:
            case SecondaryIndexOp::Full:
              assert(false);
              break;
          }
        }
        snapshot.data_item_copy = *snapshot.index_cache;
        continue;
      }
      *snapshot.index_cache = snapshot.data_item_copy;
    }

    return true;
  };

  void PostProcessing(TxStatus status) final override {
    if (status == TxStatus::Committed) {
      const EpochNumber current_epoch =
          tx_ref_.epoch_framework_ref_.GetMyThreadLocalEpoch();

      /** Unlock **/
      for (auto& snapshot : tx_ref_.write_set_ref_) {
        auto* item = snapshot.index_cache;
        auto current_tid = snapshot.data_item_copy.transaction_id.load();
        EpochNumber written_in_epoch = current_tid.epoch;

        TransactionId unlocked_id;
        if (current_epoch != written_in_epoch) {
          unlocked_id = {current_epoch, 2};
        } else {
          unlocked_id = {current_epoch, current_tid.tid + 1};
        }
        item->transaction_id.store(unlocked_id);
        snapshot.data_item_copy.transaction_id.store(unlocked_id);
        if (tx_ref_.register_deferred_purge_) {
          tx_ref_.register_deferred_purge_(snapshot, unlocked_id);
        }
      }
    }
  }

  bool ObservedReadsStillValid() const final {
    for (const auto& validation_item : validation_set_) {
      if (validation_item.item_p_cache->transaction_id.load() !=
          validation_item.transaction_id) {
        return false;
      }
    }
    return true;
  }

 private:
  bool AntiDependencyValidation() {
    for (auto& validation_item : validation_set_) {
      auto* item = validation_item.item_p_cache;
      auto tx_id = item->transaction_id.load();
      if (tx_id != validation_item.transaction_id) {
        return false;
      }
    }
    return true;
  }
};

}  // namespace ConcurrencyControl
}  // namespace LineairDB
#endif /* LINEAIRDB_SILO_H */
