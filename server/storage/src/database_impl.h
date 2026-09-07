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
 * @file server/storage/src/database_impl.h
 * The implementation behind Database: it owns the table dictionary, the
 * epoch framework, the log and the PAX version store.
 */

#ifndef HELIOS_STORAGE_SRC_DATABASE_IMPL_H
#define HELIOS_STORAGE_SRC_DATABASE_IMPL_H

#include <chrono>
#include <cstdint>
#include <functional>
#include <shared_mutex>
#include <string>
#include <utility>
#include <vector>

#include "index/reaper.h"
#include "silo/commit.h"
#include "silo/read.h"
#include "silo/snapshot.h"
#include "silo/transaction_id.h"
#include "storage/config.h"
#include "storage/database.h"
#include "table/table.h"
#include "table/table_dictionary.h"
#include "util/epoch_framework.h"
#include "wal/epoch_scan_checkpoint.h"
#include "wal/logger.h"

namespace helios::storage {

/**
 * @brief Everything a Database owns, behind its public face.
 *
 * @details The definitions are split across database.cc for the lifecycle,
 * the tables, the reads and the commit, pax/view.cc for the columnar view,
 * and index/stats.cc for the statistics.
 */
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

  EpochNumber ThreadEpoch();

  // The durable frontier and the epoch it is compared against.
  EpochNumber GetDurableEpoch() const;
  EpochNumber GetGlobalEpoch() const;

  const Config &GetConfig() const;

  // NOTE: Called by a special thread managed by epoch::Framework.
  std::function<void(EpochNumber)> EpochHook();

  bool CreateTable(const std::string_view table_name);

  pax::PaxStore *GetPaxStore(const std::string_view table_name);
  Database::PaxReadView AcquirePaxView(uint32_t fence_timeout_ms);
  void ReleasePaxView(const Database::PaxReadView &view);

  // Read view expiry, half the high-water margin. Enforces the wrap-free
  // window behind plain epoch comparisons: readers gate every attempt on
  // PaxViewPoisoned, and the cut-to-global distance grows
  // monotonically over any practical read view lifetime (a full uint32
  // epoch cycle takes years), keeping accepted results inside the bound.
  static constexpr EpochNumber kPaxReadViewEpochLifetime = 1u << 19;

  bool PaxViewPoisoned(const Database::PaxReadView &view) const;

  bool InstallPaxSchema(const std::string_view table_name,
                        const std::vector<uint32_t> &field_max_bytes,
                        const std::vector<uint8_t> &field_kind = {},
                        const std::vector<int8_t> &field_scale = {});

  bool CreateSecondaryIndex(const std::string_view table_name,
                            const std::string_view index_name,
                            const uint constraints);

  ReadResult Read(const std::string_view table_name, const std::string_view key,
                  const std::vector<uint32_t> *selected_columns = nullptr);

  std::vector<ReadResult> BatchRead(
      const std::vector<std::pair<std::string, std::string>> &keys);

  ScanResult Scan(const std::string_view table_name,
                  const std::string_view start_key,
                  const std::string_view end_key, uint64_t row_limit,
                  bool reverse_scan,
                  const std::vector<uint32_t> *selected_columns = nullptr);

  ScanIndexResult ScanIndex(
      const std::string_view table_name, const std::string_view index_name,
      const std::string_view start_key, const std::string_view end_key,
      uint64_t row_limit, bool reverse_scan,
      const std::vector<uint32_t> *selected_columns = nullptr);

  ScanPaxResult ScanPax(const std::string_view table_name,
                        const std::string_view start_key,
                        const std::string_view end_key, uint64_t row_limit,
                        bool reverse_scan);

  /**
   * @brief Computes exact NDV for each integer key-part prefix of one index.
   *
   * @details The query layer uses this to set MySQL `rec_per_key`. The scan
   * counts live index entries only. If `parts` refuses a live key, the method
   * returns false so the caller keeps its old estimate.
   */
  bool IndexNdv(const std::string_view table_name,
                const std::string_view index_name, uint32_t num_parts,
                const KeyParts &parts, std::vector<uint64_t> &out_ndv);

  /**
   * @brief Builds an equi-depth histogram for one index's leading key part.
   *
   * @details The query layer uses the returned boundaries to estimate
   * one-column range cardinality locally. The scan is independent of
   * NDV/rec_per_key: pass 1 counts row weight, and pass 2 records the
   * leading-key prefix at each bucket boundary. Secondary-index entries are
   * weighted by their PK list size so bucket depth tracks rows, not distinct
   * secondary keys. A key `parts` refuses returns false, letting the caller
   * keep its heuristic.
   */
  bool IndexHistogram(const std::string_view table_name,
                      const std::string_view index_name, uint32_t buckets,
                      const KeyParts &parts,
                      std::vector<std::string> &out_bounds,
                      std::vector<uint64_t> &out_cum);

  bool Commit(
      const std::vector<ExternalReadEntry> &reads,
      const std::vector<ExternalWriteEntry> &writes,
      const std::vector<ExternalSecondaryIndexEntry> &secondary_index_ops,
      const std::vector<ExternalRangeReadEntry> &range_reads,
      CommitDurability durability, std::string *abort_reason = nullptr);

  std::optional<Table *> GetTable(const std::string_view table_name);

  bool WriteCheckpointImage(uint64_t *out_version_retries);

 private:
  void Recovery();

 private:
  Config config_;
  wal::Logger logger_;
  epoch::Framework epoch_framework_;
  TableDictionary table_dictionary_;
  wal::EpochScanCheckpoint scan_checkpoint_;
  mutable std::shared_mutex schema_mutex_;
  index::Reaper reaper_;
};

}  // namespace helios::storage
#endif  // HELIOS_STORAGE_SRC_DATABASE_IMPL_H
