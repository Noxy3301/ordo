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

  Candidate candidate;
  candidate.kind = secondary_index == nullptr
                       ? DeferredPurgeIndexKind::Primary
                       : DeferredPurgeIndexKind::Secondary;
  candidate.primary_index = primary_index;
  candidate.secondary_index = secondary_index;
  candidate.key = std::string(key);
  candidate.item = item;
  candidate.delete_commit_tid = delete_commit_tid;

  std::lock_guard<std::mutex> lk(deferred_purge_mtx_);
  deferred_purge_candidates_.emplace_back(std::move(candidate));
}

DataItem *Reaper::Resolve(const Candidate &candidate) {
  if (candidate.kind == DeferredPurgeIndexKind::Primary) {
    return candidate.primary_index == nullptr
               ? nullptr
               : candidate.primary_index->Get(candidate.key);
  }
  return candidate.secondary_index == nullptr
             ? nullptr
             : candidate.secondary_index->Get(candidate.key);
}

bool Reaper::Erase(const Candidate &candidate, TransactionId retired_tid) {
  if (candidate.kind == DeferredPurgeIndexKind::Primary) {
    return candidate.primary_index != nullptr &&
           candidate.primary_index->Purge(candidate.key, candidate.item,
                                          retired_tid);
  }
  return candidate.secondary_index != nullptr &&
         candidate.secondary_index->Purge(candidate.key, candidate.item,
                                          retired_tid);
}

void Reaper::Reap(EpochNumber published_epoch) {
  std::vector<Candidate> ready;
  size_t pending_before = 0;
  {
    std::lock_guard<std::mutex> lk(deferred_purge_mtx_);
    pending_before = deferred_purge_candidates_.size();
    std::vector<Candidate> pending;
    pending.reserve(deferred_purge_candidates_.size());
    for (auto &candidate : deferred_purge_candidates_) {
      const EpochNumber delete_epoch = candidate.delete_commit_tid.epoch;
      const bool one_full_epoch_elapsed =
          published_epoch > delete_epoch && published_epoch - delete_epoch > 1;
      if (one_full_epoch_elapsed) {
        ready.emplace_back(std::move(candidate));
      } else {
        pending.emplace_back(std::move(candidate));
      }
    }
    deferred_purge_candidates_.swap(pending);
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

  std::vector<Candidate> requeue;
  requeue.reserve(ready.size());
  size_t reaped = 0;
  size_t requeued = 0;
  size_t dropped = 0;

  for (auto &candidate : ready) {
    DataItem *item = Resolve(candidate);
    if (item != candidate.item) {
      ++dropped;
      continue;
    }

    TransactionId observed = item->transaction_id.load();
    if (observed.tid & 1u) {
      requeue.emplace_back(std::move(candidate));
      ++requeued;
      continue;
    }
    if (!SameTransactionId(observed, candidate.delete_commit_tid)) {
      ++dropped;
      continue;
    }

    TransactionId locked = observed;
    locked.tid |= 1u;
    if (!item->transaction_id.compare_exchange_strong(observed, locked)) {
      if (observed.tid & 1u) {
        requeue.emplace_back(std::move(candidate));
        ++requeued;
      } else {
        ++dropped;
      }
      continue;
    }

    auto unlock_candidate = [&]() {
      item->transaction_id.store(candidate.delete_commit_tid);
    };

    const bool item_initialized =
        candidate.kind == DeferredPurgeIndexKind::Primary
            ? item->HasRow()
            : item->IsInitialized();
    if (item_initialized) {
      unlock_candidate();
      ++dropped;
      continue;
    }
    if (Resolve(candidate) != item) {
      unlock_candidate();
      ++dropped;
      continue;
    }
    if (!SameTransactionId(item->transaction_id.load(), locked)) {
      unlock_candidate();
      ++dropped;
      continue;
    }

    TransactionId retired = candidate.delete_commit_tid;
    retired.tid = (retired.tid + 2u) & ~1u;
    if (Erase(candidate, retired)) {
      ++reaped;
    } else {
      unlock_candidate();
      ++dropped;
    }
  }

  [[maybe_unused]] size_t pending_after = 0;
  [[maybe_unused]] uint64_t total_reaped = 0;
  [[maybe_unused]] uint64_t total_requeued = 0;
  [[maybe_unused]] uint64_t total_dropped = 0;
  {
    std::lock_guard<std::mutex> lk(deferred_purge_mtx_);
    deferred_purge_candidates_.insert(deferred_purge_candidates_.end(),
                                      std::make_move_iterator(requeue.begin()),
                                      std::make_move_iterator(requeue.end()));
    deferred_purge_reaped_ += reaped;
    deferred_purge_requeued_ += requeued;
    deferred_purge_dropped_ += dropped;
    pending_after = deferred_purge_candidates_.size();
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
