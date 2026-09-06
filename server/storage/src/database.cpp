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

#include <lineairdb/config.h>
#include <lineairdb/database.h>
#include <lineairdb/tx_status.h>

#include <algorithm>
#include <cassert>
#include <memory>

#include "database_impl.h"
#include "index/impl/masstree_index.hpp"
#include "index/secondary_index.h"
#include "recovery/flush_trace.h"
#include "util/logger.hpp"
namespace LineairDB {

Database::Database() : db_pimpl_(std::make_unique<Impl>()) {
  LineairDB::Util::SetUpSPDLog();
}
Database::Database(const Config &c) : db_pimpl_(std::make_unique<Impl>(c)) {
  LineairDB::Util::SetUpSPDLog();
}

Database::~Database() noexcept = default;

const Config Database::GetConfig() const noexcept {
  return db_pimpl_->GetConfig();
}

bool Database::SetCommitDurability(Config::CommitDurability mode,
                                   std::chrono::milliseconds barrier_timeout) {
  return db_pimpl_->SetCommitDurability(mode, barrier_timeout);
}
Config::CommitDurability Database::GetCommitDurability() const {
  return db_pimpl_->GetCommitDurability();
}
void Database::ReleaseMasstreeThreadEpoch() {
  Index::MasstreeReleaseThreadEpoch();
}
void Database::FullyDrainMasstreeThread() { Index::MasstreeFullyDrainThread(); }
bool Database::CreateTable(const std::string_view table_name) {
  return db_pimpl_->CreateTable(table_name);
}

bool Database::InstallPaxSchema(const std::string_view table_name,
                                const std::vector<uint32_t> &field_max_bytes,
                                const std::vector<uint8_t> &field_kind,
                                const std::vector<int8_t> &field_scale) {
  return db_pimpl_->InstallPaxSchema(table_name, field_max_bytes, field_kind,
                                     field_scale);
}

Pax::PaxStore *Database::GetPaxStore(const std::string_view table_name) {
  return db_pimpl_->GetPaxStore(table_name);
}

Database::PaxReadView Database::AcquirePaxReadView(uint32_t fence_timeout_ms) {
  return db_pimpl_->AcquirePaxReadView(fence_timeout_ms);
}

void Database::ReleasePaxReadView(const PaxReadView &view) {
  db_pimpl_->ReleasePaxReadView(view);
}

bool Database::PaxReadViewPoisoned(const PaxReadView &view) const {
  return db_pimpl_->PaxReadViewPoisoned(view);
}

bool Database::CreateSecondaryIndex(const std::string_view table_name,
                                    const std::string_view index_name,
                                    const uint index_type) {
  return db_pimpl_->CreateSecondaryIndex(table_name, index_name, index_type);
}

StatelessReadResult Database::StatelessRead(
    const std::string_view table_name, const std::string_view key,
    const std::vector<uint32_t> *selected_columns) {
  return db_pimpl_->StatelessRead(table_name, key, selected_columns);
}

std::vector<StatelessReadResult> Database::StatelessBatchRead(
    const std::vector<std::pair<std::string, std::string>> &keys) {
  return db_pimpl_->StatelessBatchRead(keys);
}

StatelessRangeScanResult Database::StatelessRangeScan(
    const std::string_view table_name, const std::string_view start_key,
    const std::string_view end_key, uint64_t row_limit, bool reverse_scan,
    const std::vector<uint32_t> *selected_columns) {
  return db_pimpl_->StatelessRangeScan(table_name, start_key, end_key,
                                       row_limit, reverse_scan,
                                       selected_columns);
}

StatelessPaxRowRefScanResult Database::StatelessPaxRowRefScan(
    const std::string_view table_name, const std::string_view start_key,
    const std::string_view end_key, uint64_t row_limit, bool reverse_scan) {
  return db_pimpl_->StatelessPaxRowRefScan(table_name, start_key, end_key,
                                           row_limit, reverse_scan);
}

StatelessSecondaryRangeScanResult Database::StatelessSecondaryRangeScan(
    const std::string_view table_name, const std::string_view index_name,
    const std::string_view start_key, const std::string_view end_key,
    uint64_t row_limit, bool reverse_scan,
    const std::vector<uint32_t> *selected_columns) {
  return db_pimpl_->StatelessSecondaryRangeScan(table_name, index_name,
                                                start_key, end_key, row_limit,
                                                reverse_scan, selected_columns);
}

bool Database::ComputeIndexNdvInt(const std::string_view table_name,
                                  const std::string_view index_name,
                                  uint32_t num_parts,
                                  std::vector<uint64_t> &out_ndv) {
  return db_pimpl_->ComputeIndexNdvInt(table_name, index_name, num_parts,
                                       out_ndv);
}

bool Database::ComputeIndexHistogram(const std::string_view table_name,
                                     const std::string_view index_name,
                                     uint32_t buckets,
                                     std::vector<std::string> &out_bounds,
                                     std::vector<uint64_t> &out_cum) {
  return db_pimpl_->ComputeIndexHistogram(table_name, index_name, buckets,
                                          out_bounds, out_cum);
}

bool Database::ValidateAndCommit(
    const std::vector<ExternalReadEntry> &reads,
    const std::vector<ExternalWriteEntry> &writes,
    const std::vector<ExternalSecondaryIndexEntry> &secondary_index_ops,
    const std::vector<ExternalRangeReadEntry> &range_reads,
    std::string *abort_reason) {
  return db_pimpl_->ValidateAndCommit(reads, writes, secondary_index_ops,
                                      range_reads, abort_reason);
}

bool Database::WriteCheckpointImage(uint64_t *out_version_retries) {
  return db_pimpl_->WriteCheckpointImage(out_version_retries);
}

EpochNumber Database::Impl::ResumeEpochAbove(EpochNumber frontier) {
  if (frontier >= EpochFramework::kEpochHighWater - 1) {
    SPDLOG_CRITICAL(
        "Startup failed: resuming above the recovered epoch {0} would reach "
        "the epoch high-water mark {1}",
        frontier, EpochFramework::kEpochHighWater);
    exit(EXIT_FAILURE);
  }
  return frontier + 1;
}

Config Database::Impl::NormalizeAndValidateConfig(Config config) {
  config.enable_logging =
      config.commit_durability != Config::CommitDurability::Volatile;

  return config;
}

Database::Impl::Impl(const Config &c)
    : config_(NormalizeAndValidateConfig(c)),
      logger_(config_),
      epoch_framework_(config_.epoch_duration_ms, EventsOnEpochIsUpdated()),
      scan_checkpoint_(config_, table_dictionary_, epoch_framework_, logger_) {
  if (Database::Impl::CurrentDBInstance == nullptr) {
    Database::Impl::CurrentDBInstance = this;
    SPDLOG_INFO("LineairDB instance has been constructed.");
  } else {
    SPDLOG_ERROR(
        "It is prohibited to allocate two LineairDB::Database instance at "
        "the same time.");
    exit(EXIT_FAILURE);
  }
  // Always scan the log, even without recovery: an interrupted tail has to be
  // removed before the first append lands behind it, and recovery only
  // controls whether the decoded records are replayed into the database.
  if (config_.enable_recovery) {
    Recovery();
  } else {
    auto scanned = logger_.Recover();
    if (scanned.status != Recovery::Logger::RecoveryStatus::Ok) {
      SPDLOG_CRITICAL(
          "Startup failed: the write-ahead log could not be read; refusing to "
          "start with an unknown durable state");
      exit(EXIT_FAILURE);
    }
    if (scanned.frontier != 0) {
      // Records exist that this instance will not replay; resuming at or
      // below their epoch would let a Sync commit inherit their frontier.
      epoch_framework_.SetGlobalEpoch(ResumeEpochAbove(scanned.frontier));
    }
  }
  // Armed after recovery, which reports its own failures by refusing to
  // start, and before the flusher that can raise one at run time.
  if (config_.enable_logging) logger_.EnableProcessFailStop();
  // Built before any thread records, so its storage and its dump signal are
  // in place rather than raised by whichever path happens to reach it first.
  Recovery::FlushTrace::Instance();
  logger_.StartFlusher();
  epoch_framework_.Start();
  // Last: its scan waits on the epoch it starts.
  scan_checkpoint_.Start();
}

Database::Impl::~Impl() {
  epoch_framework_.Sync();
  // Before the epoch writer stops, since a capture in progress waits for the
  // epoch to advance.
  scan_checkpoint_.Stop();
  epoch_framework_.Stop();
  // After the epoch writer has joined no further closed epoch arrives, so the
  // flusher can drain what it already owns and be joined before the log is
  // closed.
  logger_.StopAndDrainFlusher();
  SPDLOG_DEBUG(
      "Epoch number and Durable epoch number are ended at {0}, and {1}, "
      "respectively.",
      epoch_framework_.GetGlobalEpoch(), logger_.GetDurableEpoch());
  // Written once every thread that records has joined, so the census reaches
  // the filesystem without any of its cost landing on a measured path.
  Recovery::FlushTrace::Instance().Dump();
  SPDLOG_INFO("LineairDB instance has been destructed.");
  assert(Database::Impl::CurrentDBInstance == this);
  Database::Impl::CurrentDBInstance = nullptr;
}

EpochNumber Database::Impl::GetMyThreadLocalEpoch() {
  return epoch_framework_.GetMyThreadLocalEpoch();
}

const Config &Database::Impl::GetConfig() const { return config_; }

std::function<void(EpochNumber)> Database::Impl::EventsOnEpochIsUpdated() {
  return [&](EpochNumber updated_epoch) {
    // Logging. The global epoch advances from U-1 to U while threads may
    // still be online in U-1, so U-2 is the newest epoch that is certainly
    // closed and safe to write. Handing the target to the logger's own
    // flusher keeps the durability fdatasync off this pool.
    if (config_.enable_logging && updated_epoch >= 3) {
      logger_.ScheduleFlush(updated_epoch - 2);
    }

    // Tick masstree's globalepoch so RCU can free retired leaves and
    // DataItem* limbo once min_active_epoch() catches up. Workers
    // release their epoch at tx/RPC boundaries via
    // ReleaseMasstreeThreadEpoch; we only move the watermark here.
    reaper_.Reap(updated_epoch);
    Index::MasstreeAdvanceEpoch();
  };
}

bool Database::Impl::CreateTable(const std::string_view table_name) {
  return table_dictionary_.CreateTable(table_name, epoch_framework_, config_);
}

bool Database::Impl::CreateSecondaryIndex(const std::string_view table_name,
                                          const std::string_view index_name,
                                          const uint index_type) {
  std::shared_lock<std::shared_mutex> lk(schema_mutex_);
  auto it = GetTable(table_name);
  if (!it.has_value()) {
    return false;
  }
  return it.value()->CreateSecondaryIndex(
      index_name,
      Index::SecondaryIndexType::FromRaw(
          static_cast<Index::SecondaryIndexType::RawType>(index_type)));
}

StatelessReadResult Database::Impl::StatelessRead(
    const std::string_view table_name, const std::string_view key,
    const std::vector<uint32_t> *selected_columns) {
  return Silo::Read(table_dictionary_, schema_mutex_, table_name, key,
                  selected_columns);
}

std::vector<StatelessReadResult> Database::Impl::StatelessBatchRead(
    const std::vector<std::pair<std::string, std::string>> &keys) {
  return Silo::BatchRead(table_dictionary_, schema_mutex_, keys);
}

StatelessRangeScanResult Database::Impl::StatelessRangeScan(
    const std::string_view table_name, const std::string_view start_key,
    const std::string_view end_key, uint64_t row_limit, bool reverse_scan,
    const std::vector<uint32_t> *selected_columns) {
  return Silo::RangeScan(table_dictionary_, schema_mutex_, table_name, start_key,
                       end_key, row_limit, reverse_scan, selected_columns);
}

StatelessPaxRowRefScanResult Database::Impl::StatelessPaxRowRefScan(
    const std::string_view table_name, const std::string_view start_key,
    const std::string_view end_key, uint64_t row_limit, bool reverse_scan) {
  return Silo::PaxRowRefScan(table_dictionary_, schema_mutex_, table_name,
                           start_key, end_key, row_limit, reverse_scan);
}

StatelessSecondaryRangeScanResult Database::Impl::StatelessSecondaryRangeScan(
    const std::string_view table_name, const std::string_view index_name,
    const std::string_view start_key, const std::string_view end_key,
    uint64_t row_limit, bool reverse_scan,
    const std::vector<uint32_t> *selected_columns) {
  return Silo::SecondaryRangeScan(table_dictionary_, schema_mutex_, table_name,
                                index_name, start_key, end_key, row_limit,
                                reverse_scan, selected_columns);
}

bool Database::Impl::ValidateAndCommit(
    const std::vector<ExternalReadEntry> &reads,
    const std::vector<ExternalWriteEntry> &writes,
    const std::vector<ExternalSecondaryIndexEntry> &secondary_index_ops,
    const std::vector<ExternalRangeReadEntry> &range_reads,
    std::string *abort_reason) {
  const Silo::CommitPayload payload{reads, writes, secondary_index_ops,
                                  range_reads};
  return Silo::Commit(table_dictionary_, schema_mutex_, epoch_framework_, reaper_,
                    logger_, payload, GetCommitDurability(), abort_reason);
}

std::optional<Table *> Database::Impl::GetTable(
    const std::string_view table_name) {
  return table_dictionary_.GetTable(table_name);
}

bool Database::Impl::WriteCheckpointImage(uint64_t *out_version_retries) {
  Recovery::EpochScanCheckpoint::Stats stats;
  const bool published = scan_checkpoint_.RunOnce(&stats);
  if (out_version_retries != nullptr) *out_version_retries = stats.retries;
  return published;
}

void Database::Impl::RegisterDeferredPurge(const Snapshot &snapshot,
                                           TransactionId delete_commit_tid) {
  reaper_.Enqueue(snapshot, delete_commit_tid);
}

void Database::Impl::Recovery() {
  SPDLOG_INFO("Start recovery process");
  auto recovered = logger_.Recover();
  if (recovered.status != Recovery::Logger::RecoveryStatus::Ok) {
    SPDLOG_CRITICAL(
        "Recovery failed: the write-ahead log could not be read; refusing to "
        "start with an unknown durable state");
    exit(EXIT_FAILURE);
  }

  const EpochNumber durable_epoch = recovered.frontier;
  EpochNumber highest_epoch = std::max<EpochNumber>(1, durable_epoch);
  SPDLOG_DEBUG("  Durable epoch is resumed from {0}", durable_epoch);

  epoch_framework_.MakeMeOnline();
  epoch_framework_.SetMyThreadLocalEpochForRecovery(durable_epoch);

  auto &&recovery_sets = std::move(recovered.recovery_set);

  for (auto &recovery_set : recovery_sets) {
    // Skip deleted entries.
    const bool live = recovery_set.index_name.empty()
                          ? recovery_set.data_item_copy.IsPrimaryInitialized()
                          : recovery_set.data_item_copy.IsInitialized();
    if (!live) continue;
    CreateTable(recovery_set.table_name);
    auto table = GetTable(recovery_set.table_name);
    if (!table.has_value()) {
      SPDLOG_CRITICAL(
          "Recovery failed: Table {0} could not be found or created.",
          recovery_set.table_name);
      exit(EXIT_FAILURE);
    }

    highest_epoch = std::max(
        highest_epoch, recovery_set.data_item_copy.transaction_id.load().epoch);

    if (recovery_set.index_name.empty()) {
      // Primary Index recovery
      table.value()->GetPrimaryIndex().Put(
          recovery_set.key, std::move(recovery_set.data_item_copy));
    } else {
      // Secondary Index recovery
      Index::SecondaryIndex *idx = nullptr;
      table.value()->GetOrCreateSecondaryIndex(recovery_set.index_name,
                                               recovery_set.index_type, &idx);
      if (idx != nullptr) {
        SPDLOG_DEBUG(
            "  Recovery: Secondary index '{0}' restoring key '{1}' with {2} "
            "primary keys",
            recovery_set.index_name, recovery_set.key,
            recovery_set.data_item_copy.primary_keys_view().size());
        idx->Put(recovery_set.key, std::move(recovery_set.data_item_copy));
      } else {
        SPDLOG_ERROR(
            "Recovery failed: Could not create secondary index {0} for "
            "table {1}",
            recovery_set.index_name, recovery_set.table_name);
      }
    }
  }
  epoch_framework_.MakeMeOffline();

  const EpochNumber resumed_epoch = ResumeEpochAbove(highest_epoch);
  SPDLOG_DEBUG("  Global epoch is resumed from {0}", resumed_epoch);
  epoch_framework_.SetGlobalEpoch(resumed_epoch);
  SPDLOG_INFO("Finish recovery process");
}

}  // namespace LineairDB
