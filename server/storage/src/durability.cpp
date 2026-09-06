// durability.cpp
// The runtime commit-durability contract and the clocks its barrier reads.

#include <lineairdb/config.h>

#include <chrono>
#include <mutex>

#include "database_impl.h"
#include "util/debug_sync.hpp"

namespace LineairDB {

bool Database::Impl::SetCommitDurability(
    Config::CommitDurability mode, std::chrono::milliseconds barrier_timeout) {
  if (!config_.enable_logging) return false;
  if (mode == Config::CommitDurability::Volatile) return false;
  if (barrier_timeout <= std::chrono::milliseconds::zero()) return false;
  if (epoch_framework_.GetMyThreadLocalEpoch() !=
      EpochFramework::THREAD_OFFLINE) {
    return false;
  }

  // One deadline for the whole call, so a switch queued behind a long
  // barrier expires with its own caller. Holding the lock across the barrier
  // is what lets the argument below stand on one store.
  const auto deadline = std::chrono::steady_clock::now() + barrier_timeout;
  std::unique_lock<std::timed_mutex> switch_lock(durability_switch_mtx_,
                                                 std::defer_lock);
  if (!switch_lock.try_lock_until(deadline)) return false;
  if (std::chrono::steady_clock::now() >= deadline) return false;

  logger_.SetCommitDurability(mode);
  if (mode != Config::CommitDurability::Sync) return true;

  // Read after the store: a commit that captured Async did so while its
  // thread was online at its commit epoch e, and the global epoch never
  // decreases, so e <= cut.
  const EpochNumber cut = epoch_framework_.GetGlobalEpoch();
  LINEAIRDB_DEBUG_SYNC("database.before_durability_barrier");

  // At cut+2 every thread that was online at an epoch <= cut has gone
  // offline, and the epoch hook will have scheduled the flush through cut.
  if (!epoch_framework_.WaitGlobalEpochAtLeastUntil(cut + 2, deadline)) {
    return false;
  }
  if (logger_.WaitUntilDurable(cut, deadline) !=
      Recovery::Logger::WaitResult::Durable) {
    return false;
  }
  // Both waits report success for a target already reached without looking
  // at the clock, so the last word on the caller's window is here.
  return std::chrono::steady_clock::now() <= deadline;
}

Config::CommitDurability Database::Impl::GetCommitDurability() const {
  if (!config_.enable_logging) return Config::CommitDurability::Volatile;
  return logger_.GetCommitDurability();
}

EpochNumber Database::Impl::GetDurableEpoch() const {
  return logger_.GetDurableEpoch();
}

EpochNumber Database::Impl::GetGlobalEpoch() const {
  return epoch_framework_.GetGlobalEpoch();
}

}  // namespace LineairDB
