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
#ifndef HELIOS_STORAGE_SRC_DATABASE_IMPL_H
#define HELIOS_STORAGE_SRC_DATABASE_IMPL_H

#include <storage/config.h>
#include <storage/database.h>
#include <table/table.h>

#include <chrono>
#include <cstdint>
#include <functional>
#include <shared_mutex>
#include <string>
#include <utility>
#include <vector>

#include "index/reaper.h"
#include "recovery/epoch_scan_checkpoint.h"
#include "recovery/logger.h"
#include "silo/commit.h"
#include "silo/read.h"
#include "table/table_dictionary.h"
#include "types/snapshot.h"
#include "types/transaction_id.h"
#include "util/epoch_framework.h"

namespace helios::storage {

// The concurrency control this database runs.

// Bodies live in database.cc (lifecycle, tables, reads, commit),
// durability.cc, pax_view.cc, and stats.cc.
class Database::Impl {
 public:
  inline static Database::Impl *CurrentDBInstance;

 private:
  /**
   * @brief The first epoch it is safe to resume at, given a recovered
   * durability frontier.
   * @details Strictly above the frontier: a transaction joining the
   * frontier's own epoch could return a Sync acknowledgement before its
   * record was written. Refuses when even the resumed epoch would reach the
   * high-water mark; past it the epoch no longer orders against the
   * frontier, and resuming exactly at the mark would only abort on the
   * writer's first tick instead of failing diagnosably here.
   * @note Compared before the addition: the scanner accepts a frontier of
   * UINT32_MAX by design, and `frontier + 1` would wrap to zero, the value
   * that means "no participant".
   */
  static EpochNumber ResumeEpochAbove(EpochNumber frontier);

 public:
  Impl(const Config &c = Config());
  ~Impl();

  EpochNumber GetMyThreadLocalEpoch();

  // The durable frontier and the epoch it is compared against.
  EpochNumber GetDurableEpoch() const;
  EpochNumber GetGlobalEpoch() const;

  const Config &GetConfig() const;

  // NOTE: Called by a special thread managed by epoch::EpochFramework.
  std::function<void(EpochNumber)> EventsOnEpochIsUpdated();

  bool CreateTable(const std::string_view table_name);

  pax::PaxStore *GetPaxStore(const std::string_view table_name);
  Database::PaxReadView AcquirePaxReadView(uint32_t fence_timeout_ms);
  void ReleasePaxReadView(const Database::PaxReadView &view);

  // Read view expiry, half the high-water margin. Enforces the wrap-free
  // window behind plain epoch comparisons: readers gate every attempt on
  // PaxReadViewPoisoned, and the cut-to-global distance grows
  // monotonically over any practical read view lifetime (a full uint32
  // epoch cycle takes years), keeping accepted results inside the bound.
  static constexpr EpochNumber kPaxReadViewEpochLifetime = 1u << 19;

  bool PaxReadViewPoisoned(const Database::PaxReadView &view) const;

  bool InstallPaxSchema(const std::string_view table_name,
                        const std::vector<uint32_t> &field_max_bytes,
                        const std::vector<uint8_t> &field_kind = {},
                        const std::vector<int8_t> &field_scale = {});

  bool CreateSecondaryIndex(const std::string_view table_name,
                            const std::string_view index_name,
                            const uint index_type);

  StatelessReadResult Read(
      const std::string_view table_name, const std::string_view key,
      const std::vector<uint32_t> *selected_columns = nullptr);

  std::vector<StatelessReadResult> BatchRead(
      const std::vector<std::pair<std::string, std::string>> &keys);

  StatelessRangeScanResult Scan(
      const std::string_view table_name, const std::string_view start_key,
      const std::string_view end_key, uint64_t row_limit, bool reverse_scan,
      const std::vector<uint32_t> *selected_columns = nullptr);

  StatelessSecondaryRangeScanResult ScanIndex(
      const std::string_view table_name, const std::string_view index_name,
      const std::string_view start_key, const std::string_view end_key,
      uint64_t row_limit, bool reverse_scan,
      const std::vector<uint32_t> *selected_columns = nullptr);

  StatelessPaxRowRefScanResult ScanPax(const std::string_view table_name,
                                       const std::string_view start_key,
                                       const std::string_view end_key,
                                       uint64_t row_limit, bool reverse_scan);

  /**
   * @brief Compute exact NDV for each integer key-part prefix of one index.
   *
   * @details The proxy uses this to set MySQL `rec_per_key`. The scan counts
   * live index entries only. If any live key cannot be split as Helios integer
   * key parts, the method returns false so the proxy keeps its old estimate.
   */
  bool ComputeIndexNdvInt(const std::string_view table_name,
                          const std::string_view index_name, uint32_t num_parts,
                          std::vector<uint64_t> &out_ndv);

  /**
   * @brief Build an equi-depth histogram for one index's leading key part.
   *
   * @details The proxy uses the returned boundaries to estimate one-column
   * range cardinality locally. The scan is independent of NDV/rec_per_key:
   * pass 1 counts row weight, and pass 2 records the leading-key prefix at
   * each bucket boundary. Secondary-index entries are weighted by their PK
   * list size so bucket depth tracks rows, not distinct secondary keys.
   *
   * Only order-preserving fixed-layout leading parts are accepted. Unsupported
   * or malformed encodings return false, letting the proxy keep its heuristic.
   */
  bool ComputeIndexHistogram(const std::string_view table_name,
                             const std::string_view index_name,
                             uint32_t buckets,
                             std::vector<std::string> &out_bounds,
                             std::vector<uint64_t> &out_cum);

  bool ValidateAndCommit(
      const std::vector<ExternalReadEntry> &reads,
      const std::vector<ExternalWriteEntry> &writes,
      const std::vector<ExternalSecondaryIndexEntry> &secondary_index_ops,
      const std::vector<ExternalRangeReadEntry> &range_reads,
      CommitPolicy policy, std::string *abort_reason = nullptr);

  std::optional<Table *> GetTable(const std::string_view table_name);

  bool WriteCheckpointImage(uint64_t *out_version_retries);

 private:
  void Recovery();

 private:
  Config config_;
  wal::Logger logger_;
  epoch::EpochFramework epoch_framework_;
  TableDictionary table_dictionary_;
  wal::EpochScanCheckpoint scan_checkpoint_;
  mutable std::shared_mutex schema_mutex_;
  index::Reaper reaper_;
};

}  // namespace helios::storage
#endif /** HELIOS_STORAGE_SRC_DATABASE_IMPL_H **/
