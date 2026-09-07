/**
 * @file server/storage/src/index/reaper.h
 * Physical removal of the slots a commit left empty, deferred until no
 * reader can still hold a pointer to them.
 */

#ifndef HELIOS_STORAGE_SRC_INDEX_REAPER_H
#define HELIOS_STORAGE_SRC_INDEX_REAPER_H

#include <cstdint>
#include <mutex>
#include <string>
#include <string_view>
#include <vector>

#include "index/data_item.h"
#include "silo/snapshot.h"
#include "silo/transaction_id.h"
#include "util/epoch.h"

namespace helios::storage {
namespace index {

class PrimaryIndex;
class SecondaryIndex;

/**
 * @brief Physically erases logically deleted index slots after a grace
 * period.
 *
 * A committed delete leaves its DataItem in the index as a tombstone so the
 * slot keeps its TID continuity for validation; the physical Purge runs
 * here, at least one full epoch after the delete committed. Committers
 * Enqueue candidates while still holding their commit lock, and the epoch
 * hook drives Reap: enqueue now, execute on epoch advance. The shape
 * mirrors the garbage collection subsystem of the Silo reference
 * implementation (its reaper queue and last-reaped-epoch bookkeeping).
 */
class Reaper {
 public:
  /**
   * @brief Registers one logically deleted slot for a later physical purge.
   *
   * External-path form: the committer passes the already resolved owning
   * index, exactly one of primary_index / secondary_index. The call is a
   * no-op when `item` is null, `delete_commit_tid` is empty, or neither
   * index is given.
   */
  void Enqueue(PrimaryIndex *primary_index, SecondaryIndex *secondary_index,
               std::string_view key, DataItem *item,
               TransactionId delete_commit_tid);

  /**
   * @brief Purges every candidate whose delete epoch lies more than one full
   * epoch behind `published_epoch`.
   *
   * Candidates whose slot is locked are requeued; candidates whose slot was
   * reused or republished are dropped. Runs on the epoch-framework thread.
   */
  void Reap(EpochNumber published_epoch);

 private:
  // Which index owns the candidate's slot.
  enum class DeferredPurgeIndexKind { Primary, Secondary };

  /**
   * @brief One logically deleted slot awaiting its physical purge.
   *
   * Exactly one of primary_index / secondary_index is set, selected by
   * `kind`. `item` is the slot pointer observed at enqueue time and serves
   * as an identity check at reap time. `delete_commit_tid` is the TID the
   * deleting commit published on the slot; it acts both as the grace-period
   * clock and as evidence that the slot still holds the deleted version.
   */
  struct Candidate {
    DeferredPurgeIndexKind kind;
    PrimaryIndex *primary_index = nullptr;
    SecondaryIndex *secondary_index = nullptr;
    std::string key;
    DataItem *item = nullptr;
    TransactionId delete_commit_tid;
  };

  /**
   * @brief Const-correct TID equality.
   *
   * TransactionId::operator== is not const-qualified upstream, so it cannot
   * be called with a const left-hand side; this helper compares the same
   * two fields.
   */
  static bool SameTransactionId(const TransactionId &lhs,
                                const TransactionId &rhs) {
    return lhs.epoch == rhs.epoch && lhs.tid == rhs.tid;
  }

  /**
   * @brief Re-resolves the candidate's key in its owning index.
   *
   * Returns the DataItem currently installed under the key, or nullptr.
   * Reap compares the result with `candidate.item`: a mismatch means the
   * slot was already purged and re-created, so the candidate is stale.
   */
  DataItem *Resolve(const Candidate &candidate);

  /**
   * @brief Physically erases the slot through the owning index's Purge.
   *
   * `retired_tid` (delete TID + 2, lock bit clear) is stamped on the erased
   * slot so an in-place reuse continues the slot's TID sequence instead of
   * restarting below the delete TID.
   */
  bool Erase(const Candidate &candidate, TransactionId retired_tid);

  // Guards the queue: Enqueue runs on committers, Reap on the epoch thread.
  std::mutex deferred_purge_mtx_;
  std::vector<Candidate> deferred_purge_candidates_;
  // Cumulative totals for the debug log emitted by Reap.
  uint64_t deferred_purge_reaped_ = 0;
  uint64_t deferred_purge_requeued_ = 0;
  uint64_t deferred_purge_dropped_ = 0;
};

}  // namespace index
}  // namespace helios::storage

#endif  // HELIOS_STORAGE_SRC_INDEX_REAPER_H
