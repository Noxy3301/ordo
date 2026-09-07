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

/**
 * @file server/storage/src/database.cc
 * Database forwarding to its implementation, and the startup and shutdown
 * order of the epoch framework, the log and the reaper.
 */

#include "storage/database.h"

#include <algorithm>
#include <cassert>
#include <memory>

#include "database_impl.h"
#include "index/masstree_index.h"
#include "index/secondary_index.h"
#include "storage/config.h"
#include "util/spdlog.h"
#include "wal/flush_trace.h"
namespace helios::storage {

Database::Database() : db_pimpl_(std::make_unique<Impl>()) {
  helios::storage::util::InitLog();
}
Database::Database(const Config &c) : db_pimpl_(std::make_unique<Impl>(c)) {
  helios::storage::util::InitLog();
}

Database::~Database() noexcept = default;

const Config Database::GetConfig() const noexcept {
  return db_pimpl_->GetConfig();
}

void Database::ReleaseThreadEpoch() { index::MasstreeReleaseThreadEpoch(); }
void Database::DrainThread() { index::MasstreeFullyDrainThread(); }
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

pax::PaxStore *Database::GetPaxStore(const std::string_view table_name) {
  return db_pimpl_->GetPaxStore(table_name);
}

Database::PaxReadView Database::AcquirePaxView(uint32_t fence_timeout_ms) {
  return db_pimpl_->AcquirePaxView(fence_timeout_ms);
}

void Database::ReleasePaxView(const PaxReadView &view) {
  db_pimpl_->ReleasePaxView(view);
}

bool Database::PaxViewPoisoned(const PaxReadView &view) const {
  return db_pimpl_->PaxViewPoisoned(view);
}

bool Database::CreateSecondaryIndex(const std::string_view table_name,
                                    const std::string_view index_name,
                                    const uint constraints) {
  return db_pimpl_->CreateSecondaryIndex(table_name, index_name, constraints);
}

bool Database::HasTable(const std::string_view table_name) {
  return db_pimpl_->GetTable(table_name).has_value();
}

ReadResult Database::Read(const std::string_view table_name,
                          const std::string_view key,
                          const std::vector<uint32_t> *selected_columns) {
  return db_pimpl_->Read(table_name, key, selected_columns);
}

std::vector<ReadResult> Database::BatchRead(
    const std::vector<std::pair<std::string, std::string>> &keys) {
  return db_pimpl_->BatchRead(keys);
}

ScanResult Database::Scan(const std::string_view table_name,
                          const std::string_view start_key,
                          const std::string_view end_key, uint64_t row_limit,
                          bool reverse_scan,
                          const std::vector<uint32_t> *selected_columns) {
  return db_pimpl_->Scan(table_name, start_key, end_key, row_limit,
                         reverse_scan, selected_columns);
}

ScanIndexResult Database::ScanIndex(
    const std::string_view table_name, const std::string_view index_name,
    const std::string_view start_key, const std::string_view end_key,
    uint64_t row_limit, bool reverse_scan,
    const std::vector<uint32_t> *selected_columns) {
  return db_pimpl_->ScanIndex(table_name, index_name, start_key, end_key,
                              row_limit, reverse_scan, selected_columns);
}

ScanPaxResult Database::ScanPax(const std::string_view table_name,
                                const std::string_view start_key,
                                const std::string_view end_key,
                                uint64_t row_limit, bool reverse_scan) {
  return db_pimpl_->ScanPax(table_name, start_key, end_key, row_limit,
                            reverse_scan);
}

bool Database::IndexNdv(const std::string_view table_name,
                        const std::string_view index_name, uint32_t num_parts,
                        const KeyParts &parts, std::vector<uint64_t> &out_ndv) {
  return db_pimpl_->IndexNdv(table_name, index_name, num_parts, parts, out_ndv);
}

bool Database::IndexHistogram(const std::string_view table_name,
                              const std::string_view index_name,
                              uint32_t buckets, const KeyParts &parts,
                              std::vector<std::string> &out_bounds,
                              std::vector<uint64_t> &out_cum) {
  return db_pimpl_->IndexHistogram(table_name, index_name, buckets, parts,
                                   out_bounds, out_cum);
}

bool Database::Commit(
    const std::vector<ExternalReadEntry> &reads,
    const std::vector<ExternalWriteEntry> &writes,
    const std::vector<ExternalSecondaryIndexEntry> &secondary_index_ops,
    const std::vector<ExternalRangeReadEntry> &range_reads,
    CommitDurability durability, std::string *abort_reason) {
  return db_pimpl_->Commit(reads, writes, secondary_index_ops, range_reads,
                           durability, abort_reason);
}

bool Database::WriteCheckpointImage(uint64_t *out_version_retries) {
  return db_pimpl_->WriteCheckpointImage(out_version_retries);
}

EpochNumber Database::Impl::ResumeEpochAbove(EpochNumber frontier) {
  if (frontier >= epoch::Framework::kEpochHighWater - 1) {
    SPDLOG_CRITICAL(
        "Startup failed: resuming above the recovered epoch {0} would reach "
        "the epoch high-water mark {1}",
        frontier, epoch::Framework::kEpochHighWater);
    exit(EXIT_FAILURE);
  }
  return frontier + 1;
}

Database::Impl::Impl(const Config &c)
    : config_(c),
      logger_(config_),
      epoch_framework_(config_.epoch_duration_ms, EpochHook()),
      scan_checkpoint_(config_, table_dictionary_, epoch_framework_, logger_) {
  if (Database::Impl::CurrentDBInstance == nullptr) {
    Database::Impl::CurrentDBInstance = this;
    SPDLOG_INFO("LineairDB instance has been constructed.");
  } else {
    SPDLOG_ERROR(
        "It is prohibited to allocate two helios::storage::Database instance "
        "at "
        "the same time.");
    exit(EXIT_FAILURE);
  }
  // Always scan the log, even without recovery: an interrupted tail has to be
  // removed before the first append lands behind it, and recovery only
  // controls whether the records the scan read are replayed.
  if (config_.enable_recovery) {
    Recovery();
  } else {
    auto scanned = logger_.Recover();
    if (scanned.status != wal::Logger::RecoveryStatus::Ok) {
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
  logger_.SetFailStop();
  // Built before any thread records, so its storage and its dump signal are
  // in place rather than raised by whichever path happens to reach it first.
  wal::FlushTrace::Instance();
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
  logger_.StopFlusher();
  SPDLOG_DEBUG(
      "Epoch number and Durable epoch number are ended at {0}, and {1}, "
      "respectively.",
      epoch_framework_.GetGlobalEpoch(), logger_.GetDurableEpoch());
  // Written once every thread that records has joined, so the census reaches
  // the filesystem without any of its cost landing on a measured path.
  wal::FlushTrace::Instance().Dump();
  SPDLOG_INFO("LineairDB instance has been destructed.");
  assert(Database::Impl::CurrentDBInstance == this);
  Database::Impl::CurrentDBInstance = nullptr;
}

EpochNumber Database::Impl::ThreadEpoch() {
  return epoch_framework_.ThreadEpoch();
}

const Config &Database::Impl::GetConfig() const { return config_; }

std::function<void(EpochNumber)> Database::Impl::EpochHook() {
  return [&](EpochNumber updated_epoch) {
    // Logging. The global epoch advances from U-1 to U while threads may
    // still be online in U-1, so U-2 is the newest epoch that is certainly
    // closed and safe to write. Handing the target to the logger's own
    // flusher keeps the durability fdatasync off this pool.
    if (updated_epoch >= 3) {
      logger_.ScheduleFlush(updated_epoch - 2);
    }

    // Tick masstree's globalepoch so RCU can free retired leaves and
    // DataItem* limbo once min_active_epoch() catches up. Workers
    // release their epoch at tx/RPC boundaries via
    // ReleaseThreadEpoch; this call only moves the watermark.
    reaper_.Reap(updated_epoch);
    index::MasstreeAdvanceEpoch();
  };
}

bool Database::Impl::CreateTable(const std::string_view table_name) {
  return table_dictionary_.CreateTable(table_name, epoch_framework_, config_);
}

bool Database::Impl::CreateSecondaryIndex(const std::string_view table_name,
                                          const std::string_view index_name,
                                          const uint constraints) {
  if (constraints > index::IndexConstraint::kUnique) return false;
  // Exclusive: every reader of the definition holds this lock shared, so a
  // shared one here would let a request resolve half of a schema change.
  std::unique_lock<std::shared_mutex> lk(schema_mutex_);
  auto it = GetTable(table_name);
  if (!it.has_value()) {
    return false;
  }
  return it.value()->CreateSecondaryIndex(
      index_name,
      index::IndexConstraint::FromRaw(
          static_cast<index::IndexConstraint::RawType>(constraints)));
}

ReadResult Database::Impl::Read(const std::string_view table_name,
                                const std::string_view key,
                                const std::vector<uint32_t> *selected_columns) {
  return silo::Read(table_dictionary_, schema_mutex_, table_name, key,
                    selected_columns);
}

std::vector<ReadResult> Database::Impl::BatchRead(
    const std::vector<std::pair<std::string, std::string>> &keys) {
  return silo::BatchRead(table_dictionary_, schema_mutex_, keys);
}

ScanResult Database::Impl::Scan(const std::string_view table_name,
                                const std::string_view start_key,
                                const std::string_view end_key,
                                uint64_t row_limit, bool reverse_scan,
                                const std::vector<uint32_t> *selected_columns) {
  return silo::Scan(table_dictionary_, schema_mutex_, table_name, start_key,
                    end_key, row_limit, reverse_scan, selected_columns);
}

ScanIndexResult Database::Impl::ScanIndex(
    const std::string_view table_name, const std::string_view index_name,
    const std::string_view start_key, const std::string_view end_key,
    uint64_t row_limit, bool reverse_scan,
    const std::vector<uint32_t> *selected_columns) {
  return silo::ScanIndex(table_dictionary_, schema_mutex_, table_name,
                         index_name, start_key, end_key, row_limit,
                         reverse_scan, selected_columns);
}

ScanPaxResult Database::Impl::ScanPax(const std::string_view table_name,
                                      const std::string_view start_key,
                                      const std::string_view end_key,
                                      uint64_t row_limit, bool reverse_scan) {
  return silo::ScanPax(table_dictionary_, schema_mutex_, table_name, start_key,
                       end_key, row_limit, reverse_scan);
}

bool Database::Impl::Commit(
    const std::vector<ExternalReadEntry> &reads,
    const std::vector<ExternalWriteEntry> &writes,
    const std::vector<ExternalSecondaryIndexEntry> &secondary_index_ops,
    const std::vector<ExternalRangeReadEntry> &range_reads,
    CommitDurability durability, std::string *abort_reason) {
  const silo::CommitPayload payload{reads, writes, secondary_index_ops,
                                    range_reads};
  return silo::Commit(table_dictionary_, schema_mutex_, epoch_framework_,
                      reaper_, logger_, payload, durability, abort_reason);
}

EpochNumber Database::Impl::GetDurableEpoch() const {
  return logger_.GetDurableEpoch();
}

EpochNumber Database::Impl::GetGlobalEpoch() const {
  return epoch_framework_.GetGlobalEpoch();
}

std::optional<Table *> Database::Impl::GetTable(
    const std::string_view table_name) {
  return table_dictionary_.GetTable(table_name);
}

bool Database::Impl::WriteCheckpointImage(uint64_t *out_version_retries) {
  wal::EpochScanCheckpoint::Stats stats;
  const bool published = scan_checkpoint_.RunOnce(&stats);
  if (out_version_retries != nullptr) *out_version_retries = stats.retries;
  return published;
}

void Database::Impl::Recovery() {
  SPDLOG_INFO("Start recovery process");
  auto recovered = logger_.Recover();
  if (recovered.status != wal::Logger::RecoveryStatus::Ok) {
    SPDLOG_CRITICAL(
        "Recovery failed: the write-ahead log could not be read; refusing to "
        "start with an unknown durable state");
    exit(EXIT_FAILURE);
  }

  const EpochNumber durable_epoch = recovered.frontier;
  EpochNumber highest_epoch = std::max<EpochNumber>(1, durable_epoch);
  SPDLOG_DEBUG("  Durable epoch is resumed from {0}", durable_epoch);

  epoch_framework_.Join();
  epoch_framework_.SetThreadEpoch(durable_epoch);

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
      index::SecondaryIndex *idx = nullptr;
      table.value()->GetOrCreateIndex(recovery_set.index_name,
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
  epoch_framework_.Leave();

  const EpochNumber resumed_epoch = ResumeEpochAbove(highest_epoch);
  SPDLOG_DEBUG("  Global epoch is resumed from {0}", resumed_epoch);
  epoch_framework_.SetGlobalEpoch(resumed_epoch);
  SPDLOG_INFO("Finish recovery process");
}

}  // namespace helios::storage
