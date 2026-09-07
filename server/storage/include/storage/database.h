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

#ifndef HELIOS_STORAGE_INCLUDE_STORAGE_DATABASE_H
#define HELIOS_STORAGE_INCLUDE_STORAGE_DATABASE_H

#include <storage/config.h>
#include <storage/read.h>

#include <chrono>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "config.h"

namespace helios::storage {

namespace pax {
class PaxStore;
}

class Database {
 public:
  /**
   * @brief Construct a new Database object. Thread-safe.
   * Note that a default-constructed Config object will be passed.
   */
  Database();

  /**
   * @brief Construct a new Database object. Thread-safe.
   * @param config See Config for more details of configuration.
   * @throws std::system_error when the working directory cannot be opened,
   * which includes another process already holding the log's exclusive lock.
   * The caller decides what to do about it; a noexcept constructor would
   * terminate instead.
   */
  Database(const Config &config);

  ~Database() noexcept;
  Database(const Database &) = delete;
  Database &operator=(const Database &) = delete;
  Database(Database &&) = delete;
  Database &operator=(Database &&) = delete;

  /**
   * @brief Return the Config object set by constructor.
   * @return Config object. Note that it is not an lvalue reference
   * and you cannot change the configuration with it.
   * Thread-safe.
   */
  const Config GetConfig() const noexcept;

  /**
   * @brief End the calling thread's masstree RCU critical section, drain
   * the now-eligible entries from its limbo list, and drop it from the
   * `min_active_epoch()` participant set.
   *
   * The caller MUST guarantee that no raw DataItem* (or masstree leaf
   * pointer) obtained inside the section is still in use past this call —
   * a section-end is a release operation in the RCU sense, after which
   * other threads' physical deletes can free those objects.
   *
   * The first masstree op on the thread (after construction or after a
   * release) implicitly re-opens a section at the then-current
   * globalepoch; there is no separate "begin" call.
   */
  void ReleaseThreadEpoch();

  /**
   * @brief Like ReleaseThreadEpoch but pessimistically advances
   * the global masstree epoch so the calling thread's RCU limbo is fully
   * drained before it returns. Intended for the connection-close path
   * only; substantially heavier than a regular release at high
   * concurrency.
   */
  void DrainThread();

  bool CreateSecondaryIndex(const std::string_view table_name,
                            const std::string_view index_name,
                            const uint index_type);

  // True when the dictionary holds this table.
  bool HasTable(const std::string_view table_name);

  /**
   * @brief
   * Creates a new table.
   * @param[in] table_name The name of the table to create.
   * @return true when a new table is created (the name was not previously
   * used).
   * @return false when no table is created because the table name already
   * exists.
   */
  bool CreateTable(const std::string_view table_name);

  /**
   * @brief Enables PAX storage for rows created after the call.
   *
   * @details Installs the per-field maximum cell widths: index 0 is the row
   * format's null-flags field, followed by one entry per column in field order.
   * Rows written after installation are stored in per-column strips when they
   * fit the configured cell widths; oversize rows fall back to heap storage.
   * Call once after CreateTable and before loading rows.
   *
   * @param[in] table_name The table that should use PAX storage.
   * @param[in] field_max_bytes Maximum encoded bytes for each row field.
   * @return true when the schema is installed for the table.
   * @return false when the table is missing, the schema is empty, unsupported
   * by the configured index backend, or already installed.
   */
  bool InstallPaxSchema(const std::string_view table_name,
                        const std::vector<uint32_t> &field_max_bytes,
                        const std::vector<uint8_t> &field_kind = {},
                        const std::vector<int8_t> &field_scale = {});

  /**
   * @brief Returns the PAX store installed for `table_name`.
   *
   * @param table_name Target table.
   * @return Store pointer, or nullptr when the table is missing or has no PAX
   * schema.
   */
  pax::PaxStore *GetPaxStore(const std::string_view table_name);

  /**
   * @brief Handle for one columnar read view.
   *
   * @details `cut_epoch` is the read view's serialization point: commits
   * with epoch <= cut are visible, later ones resolve to before-images.
   * `token` must be passed back to ReleasePaxView exactly once.
   */
  struct PaxReadView {
    bool valid = false;
    uint32_t cut_epoch = 0;
    uint64_t token = 0;
    std::string error;  // rejection reason when !valid
  };

  /**
   * @brief Arms before-image capture and fences the epoch so `cut_epoch`
   * is a sound serialization point.
   *
   * @details On return every commit with epoch <= cut_epoch has finished
   * installing, and every later commit captures the rows it overwrites or
   * poisons the read view. The calling thread must not hold an epoch (it
   * must be outside any transaction). Fails instead of falling back on
   * fence timeout, near the epoch high-water mark, or when the generation
   * is poisoned during acquisition.
   *
   * @param fence_timeout_ms Upper bound on the fence wait.
   * @return A valid handle, or an invalid one carrying the reason.
   */
  PaxReadView AcquirePaxView(uint32_t fence_timeout_ms);

  /**
   * @brief Releases a read view; the last active release clears the undo
   * maps. Safe to call with an invalid handle (no-op).
   */
  void ReleasePaxView(const PaxReadView &view);

  /**
   * @brief Returns whether this read view's results must be discarded.
   *
   * @details True when the capture generation was poisoned or the read view
   * outlived its epoch-lifetime bound. Callers gate every result on this
   * before accepting it.
   */
  bool PaxViewPoisoned(const PaxReadView &view) const;

  // ----------------------------------------------------------------------
  // Reads, scans and the commit.
  //
  // The methods below hold no state between calls. Each returns the snapshot
  // the caller needs (value, packed TID) so that the caller can keep its own
  // read set across independent RPCs. The collected snapshot is replayed
  // through Commit when the logical transaction is ready to
  // commit.
  // See @ref read.h for the supporting types.
  // ----------------------------------------------------------------------

  /**
   * @brief Read one row without opening a server-side transaction.
   *
   * Looks the key up in the primary index of `table_name` and returns the
   * current value together with the packed TID observed at read time. The
   * caller should later pass the same TID back inside an ExternalReadEntry
   * so that Commit can confirm the row was not modified
   * concurrently.
   *
   * @param table_name Target table.
   * @param key Primary key to look up.
   * @param selected_columns Optional zero-based MySQL columns to materialize
   * for PAX-resident rows. Unselected PAX columns are returned as empty fields.
   * @return Result with `found` set when the key exists and was non-empty.
   *         When the table does not exist, `found` is false and `tid` is 0.
   */
  ReadResult Read(const std::string_view table_name, const std::string_view key,
                  const std::vector<uint32_t> *selected_columns = nullptr);

  /**
   * @brief Read several rows in one call.
   *
   * Each `keys[i] = {table_name, key}` is resolved with the same protocol as
   * Read. Reads do not share state, so this is purely a transport
   * optimization on top of repeated Read calls.
   *
   * @param keys (table_name, key) pairs to look up.
   * @return One ReadResult per input, in the same order.
   */
  std::vector<ReadResult> BatchRead(
      const std::vector<std::pair<std::string, std::string>> &keys);

  /**
   * @brief Range-scan the primary index and return the rows observed in
   *        the range.
   *
   * Each returned row carries its own TID. To revalidate the range at
   * commit, assemble an ExternalRangeReadEntry from this call's arguments
   * and the returned keys, and register every consumed row as an
   * ExternalReadEntry.
   *
   * @param table_name Target table.
   * @param start_key Inclusive start of the range.
   * @param end_key   Exclusive end of the range. Must be non-empty.
   * @param row_limit Maximum rows to return. 0 means no cap.
   * @param reverse_scan When true, iterate from `end_key` toward `start_key`.
   * @param selected_columns Optional zero-based MySQL columns to materialize
   * for PAX-resident rows. Unselected PAX columns are returned as empty fields.
   * @return Result with `ok == false` if the table is missing or `end_key`
   *         is empty. Callers should treat `!ok` as an abort signal.
   */
  ScanResult Scan(const std::string_view table_name,
                  const std::string_view start_key,
                  const std::string_view end_key, uint64_t row_limit,
                  bool reverse_scan,
                  const std::vector<uint32_t> *selected_columns = nullptr);

  /**
   * @brief Range-scan a secondary index and resolve each hit to its base row.
   *
   * For every secondary key in `[start_key, end_key)`, this resolves each of
   * its primary keys, reads the base row, and reports
   * `{secondary_key, primary_key, value, tid, found}` per result. To
   * revalidate the range at commit, assemble an ExternalRangeReadEntry from
   * this call's arguments and both returned key lists; each base row carries
   * its TID for revalidation as a point read.
   *
   * @param table_name Base table.
   * @param index_name Secondary index name.
   * @param start_key Inclusive start of the secondary range.
   * @param end_key Exclusive end of the secondary range. Must be non-empty.
   * @param row_limit Maximum rows to return. 0 means no cap.
   * @param reverse_scan When true, iterate in reverse secondary-key order.
   * @param selected_columns Optional zero-based MySQL columns to materialize
   * for PAX-resident base rows. Unselected PAX columns are returned as empty
   * fields.
   * @return Result with `ok == false` if the table or the index is missing,
   *         or `end_key` is empty.
   */
  ScanIndexResult ScanIndex(
      const std::string_view table_name, const std::string_view index_name,
      const std::string_view start_key, const std::string_view end_key,
      uint64_t row_limit, bool reverse_scan,
      const std::vector<uint32_t> *selected_columns = nullptr);

  /**
   * @brief Range-scans the primary index and returns PAX cell references.
   *
   * @details The returned rows are not materialized. `ok == false` means the
   * caller must fall back to Scan.
   *
   * @param table_name Target table.
   * @param start_key Inclusive start of the range.
   * @param end_key Exclusive end of the range. Must be non-empty.
   * @param row_limit Maximum live rows to return. 0 means no cap.
   * @param reverse_scan When true, iterate in reverse key order.
   */
  ScanPaxResult ScanPax(const std::string_view table_name,
                        const std::string_view start_key,
                        const std::string_view end_key, uint64_t row_limit,
                        bool reverse_scan);

  /**
   * @brief Compute per-key-part-prefix NDV for an integer encoded index.
   *
   * `out_ndv[d]` is the number of distinct prefixes covering key parts
   * `0..d` among live entries. `index_name == ""` selects the primary index.
   * Returns false when the table/index is missing or any scanned key part is
   * not in the Helios integer key encoding, leaving the caller to use its
   * existing heuristic.
   */
  bool IndexNdv(const std::string_view table_name,
                const std::string_view index_name, uint32_t num_parts,
                std::vector<uint64_t> &out_ndv);

  /**
   * @brief Build an equi-depth histogram for one index's leading key part.
   *
   * @details `out_bounds[i]` is the raw encoded leading-key prefix for a
   * bucket boundary, in ascending order. `out_cum[i]` is the cumulative row
   * count up to that boundary and is monotone; the last value is the total
   * counted rows. `index_name == ""` selects the primary index. Returns false
   * when the table/index is missing, the index is empty, or a leading key part
   * cannot be decoded safely.
   */
  bool ComputeIndexHistogram(const std::string_view table_name,
                             const std::string_view index_name,
                             uint32_t buckets,
                             std::vector<std::string> &out_bounds,
                             std::vector<uint64_t> &out_cum);

  /**
   * @brief Validate caller-supplied read and write sets and install the
   *        writes atomically.
   *
   * Runs the Silo commit protocol against external inputs: resolve each
   * key to its record, lock the write set, validate the reads (point
   * TIDs, range replays, UNIQUE rechecks), then install the writes,
   * append the log set, and unlock with a new TID. The full contract
   * lives with silo::Commit.
   *
   * Aborts return false. The optional `abort_reason` is set to a short
   * machine-readable label such as `exact_read_tid_moved`,
   * `primary_range_result_changed`, or `unique_si_exists_after_lock`.
   *
   * @param reads Point reads to revalidate before commit.
   * @param writes Row writes (`is_delete == true` to remove the row).
   * @param secondary_index_ops Secondary-index adds/removes to install.
   * @param range_reads Range reads assembled by the caller from earlier
   *                    scans.
   * @param policy When this commit is acknowledged, relative to its record
   *                    reaching the device.
   * @param abort_reason Optional out parameter. Set only when the function
   *                    returns false.
   * @return true on commit; false on validation failure or schema mismatch.
   */
  bool Commit(
      const std::vector<ExternalReadEntry> &reads,
      const std::vector<ExternalWriteEntry> &writes,
      const std::vector<ExternalSecondaryIndexEntry> &secondary_index_ops,
      const std::vector<ExternalRangeReadEntry> &range_reads,
      CommitPolicy policy, std::string *abort_reason = nullptr);

  /**
   * @brief Writes one image of the live rows, on the calling thread.
   *
   * Does what the configured checkpoint interval does, at a moment the caller
   * chooses. The caller must not be inside a transaction: the scan waits for
   * the epoch to advance past every transaction in flight, and its own would
   * hold that open.
   *
   * @param out_version_retries Optional out parameter, set to how many times a
   *        row had to be read again because a writer held it or changed it
   *        during the copy.
   * @return true when an image was published.
   * @return false when no image was published, or when the final directory
   *         sync failed after the atomic rename, in which case the new image
   *         is in place but its publication is not yet durable.
   */
  bool WriteCheckpointImage(uint64_t *out_version_retries = nullptr);

  class Impl;

 private:
  const std::unique_ptr<Impl> db_pimpl_;
};
}  // namespace helios::storage

#endif  // HELIOS_STORAGE_INCLUDE_STORAGE_DATABASE_H
