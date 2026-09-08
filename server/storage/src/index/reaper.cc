/**
 * @file server/storage/src/index/reaper.cc
 * The deferred purge queue: candidates wait for an epoch no reader can be
 * in before their slots are erased.
 */

#include "index/reaper.h"

#include <iterator>
#include <utility>

#include "index/masstree_index.h"
#include "index/primary_index.h"
#include "index/secondary_index.h"
#include "util/spdlog.h"

namespace helios::storage {
namespace index {

void Reaper::Enqueue(PrimaryIndex *primary_index,
                     SecondaryIndex *secondary_index, std::string_view key,
                     DataItem *item, TransactionId delete_commit_tid) {
  if (item == nullptr || delete_commit_tid.IsEmpty()) return;
  if (primary_index == nullptr && secondary_index == nullptr) return;

  Tombstone tombstone;
  tombstone.kind = secondary_index == nullptr
                       ? DeferredPurgeIndexKind::Primary
                       : DeferredPurgeIndexKind::Secondary;
  tombstone.primary_index = primary_index;
  tombstone.secondary_index = secondary_index;
  tombstone.key = std::string(key);
  tombstone.item = item;
  tombstone.delete_commit_tid = delete_commit_tid;

  std::lock_guard<std::mutex> lk(mtx_);
  tombstones_.emplace_back(std::move(tombstone));
}

DataItem *Reaper::Get(const Tombstone &tombstone) {
  if (tombstone.kind == DeferredPurgeIndexKind::Primary) {
    return tombstone.primary_index == nullptr
               ? nullptr
               : tombstone.primary_index->Get(tombstone.key);
  }
  return tombstone.secondary_index == nullptr
             ? nullptr
             : tombstone.secondary_index->Get(tombstone.key);
}

bool Reaper::Purge(const Tombstone &tombstone, TransactionId retired_tid) {
  if (tombstone.kind == DeferredPurgeIndexKind::Primary) {
    return tombstone.primary_index != nullptr &&
           tombstone.primary_index->Purge(tombstone.key, tombstone.item,
                                          retired_tid);
  }
  return tombstone.secondary_index != nullptr &&
         tombstone.secondary_index->Purge(tombstone.key, tombstone.item,
                                          retired_tid);
}

void Reaper::Reap(EpochNumber published_epoch) {
  std::vector<Tombstone> ready;
  size_t pending_before = 0;
  {
    std::lock_guard<std::mutex> lk(mtx_);
    pending_before = tombstones_.size();
    std::vector<Tombstone> pending;
    pending.reserve(tombstones_.size());
    for (auto &tombstone : tombstones_) {
      const EpochNumber delete_epoch = tombstone.delete_commit_tid.epoch;
      const bool one_full_epoch_elapsed =
          published_epoch > delete_epoch && published_epoch - delete_epoch > 1;
      if (one_full_epoch_elapsed) {
        ready.emplace_back(std::move(tombstone));
      } else {
        pending.emplace_back(std::move(tombstone));
      }
    }
    tombstones_.swap(pending);
  }

  if (ready.empty()) {
    if (pending_before != 0) {
      SPDLOG_DEBUG(
          "Deferred purge epoch={} pending={} reaped=0 requeued=0 "
          "dropped=0 total_reaped={} total_requeued={} total_dropped={}",
          published_epoch, pending_before, deferred_purge_reaped_,
          deferred_purge_requeued_, deferred_purge_dropped_);
    }
    return;
  }

  std::vector<Tombstone> requeue;
  requeue.reserve(ready.size());
  size_t reaped = 0;
  size_t requeued = 0;
  size_t dropped = 0;

  for (auto &tombstone : ready) {
    DataItem *item = Get(tombstone);
    if (item != tombstone.item) {
      ++dropped;
      continue;
    }

    TransactionId observed = item->transaction_id.load();
    if (observed.tid & 1u) {
      requeue.emplace_back(std::move(tombstone));
      ++requeued;
      continue;
    }
    if (!SameTransactionId(observed, tombstone.delete_commit_tid)) {
      ++dropped;
      continue;
    }

    TransactionId locked = observed;
    locked.tid |= 1u;
    if (!item->transaction_id.compare_exchange_strong(observed, locked)) {
      if (observed.tid & 1u) {
        requeue.emplace_back(std::move(tombstone));
        ++requeued;
      } else {
        ++dropped;
      }
      continue;
    }

    auto unlock = [&]() {
      item->transaction_id.store(tombstone.delete_commit_tid);
    };

    const bool item_initialized =
        tombstone.kind == DeferredPurgeIndexKind::Primary
            ? item->HasRow()
            : item->IsInitialized();
    if (item_initialized) {
      unlock();
      ++dropped;
      continue;
    }
    if (Get(tombstone) != item) {
      unlock();
      ++dropped;
      continue;
    }
    if (!SameTransactionId(item->transaction_id.load(), locked)) {
      unlock();
      ++dropped;
      continue;
    }

    TransactionId retired = tombstone.delete_commit_tid;
    retired.tid = (retired.tid + 2u) & ~1u;
    if (Purge(tombstone, retired)) {
      ++reaped;
    } else {
      unlock();
      ++dropped;
    }
  }

  [[maybe_unused]] size_t pending_after = 0;
  [[maybe_unused]] uint64_t total_reaped = 0;
  [[maybe_unused]] uint64_t total_requeued = 0;
  [[maybe_unused]] uint64_t total_dropped = 0;
  {
    std::lock_guard<std::mutex> lk(mtx_);
    tombstones_.insert(tombstones_.end(),
                       std::make_move_iterator(requeue.begin()),
                       std::make_move_iterator(requeue.end()));
    deferred_purge_reaped_ += reaped;
    deferred_purge_requeued_ += requeued;
    deferred_purge_dropped_ += dropped;
    pending_after = tombstones_.size();
    total_reaped = deferred_purge_reaped_;
    total_requeued = deferred_purge_requeued_;
    total_dropped = deferred_purge_dropped_;
  }

  SPDLOG_DEBUG(
      "Deferred purge epoch={} pending={} reaped={} requeued={} dropped={} "
      "total_reaped={} total_requeued={} total_dropped={}",
      published_epoch, pending_after, reaped, requeued, dropped, total_reaped,
      total_requeued, total_dropped);

  MasstreeReleaseThreadEpoch();
}

}  // namespace index
}  // namespace helios::storage
