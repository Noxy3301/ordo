#ifndef HELIOS_STORAGE_INCLUDE_STORAGE_READ_H
#define HELIOS_STORAGE_INCLUDE_STORAGE_READ_H

#include <cstdint>
#include <string>
#include <vector>

namespace helios::storage {

/**
 * @file server/storage/include/storage/read.h
 * @brief Result types of the reads and scans, the commit entries and the
 * commit policy.
 */

// ---------------------------------------------------------------------
// Read and scan outcomes: pure observations.
// ---------------------------------------------------------------------

/**
 * @brief Outcome of a single Database::Read.
 *
 * `tid` is the packed (epoch:32 | tid:32) version observed at read time.
 * Resubmit the same `tid` through Commit inside an
 * ExternalReadEntry to assert that the row did not move before commit.
 */
struct ReadResult {
  bool found = false;  ///< True when the key existed and was non-empty.
  std::string value;   ///< Row payload, valid only when @ref found is true.
  uint64_t tid = 0;    ///< Packed (epoch | tid) version observed at read time.
};

/**
 * @brief One row from a primary-index range scan.
 */
struct ScanRow {
  std::string key;
  std::string value;
  uint64_t tid = 0;    ///< Packed version observed for this row.
  bool found = false;  ///< Tombstones are normally filtered out before this
                       ///< struct is produced.
};

/**
 * @brief One row from a secondary-index range scan.
 *
 * `secondary_key` is the indexed key, `primary_key` is the base-table key
 * reached through it, and `value` is the corresponding base-table row.
 */
struct ScanIndexRow {
  std::string secondary_key;
  std::string primary_key;
  std::string value;
  uint64_t tid = 0;  ///< Packed version of the base row.
  bool found = false;
};

/**
 * @brief Outcome of Database::Scan.
 *
 * `ok` separates a genuine empty result from a scan that never ran: the
 * table does not exist, or the exclusive end bound is empty. On
 * `ok == false` the caller should abort the logical transaction.
 */
struct ScanResult {
  bool ok = false;
  std::vector<ScanRow> rows;
};

/**
 * @brief One row reference from a PAX primary-index range scan.
 *
 * @details `group` is a `pax::PaxGroup*` and `item` is a `DataItem*`, kept
 * opaque so this public header does not expose internal storage headers.
 * Callers read the cells they need, then call CurrentTid() and compare
 * the result with `tid` to reject torn reads.
 */
struct ScanPaxRow {
  std::string key;
  const void *group = nullptr;
  uint32_t slot = 0;
  uint32_t row_size = 0;
  uint64_t tid = 0;
  const void *item = nullptr;
};

/**
 * @brief Outcome of a PAX primary-index range scan.
 *
 * @details `ok == false` means the caller must use the materializing
 * Scan path instead. This happens when the end bound is empty,
 * the table is missing, it has no PAX store, or it contains heap-fallback
 * rows.
 */
struct ScanPaxResult {
  bool ok = false;
  std::vector<ScanPaxRow> rows;
};

/**
 * @brief Returns the current packed TID for a PAX row reference.
 *
 * @param row Row reference returned by Database::ScanPax.
 */
uint64_t CurrentTid(const ScanPaxRow &row);

/**
 * @brief Outcome of Database::ScanIndex.
 */
struct ScanIndexResult {
  bool ok = false;
  std::vector<ScanIndexRow> rows;
};

// ---------------------------------------------------------------------
// Commit inputs: the caller assembles these from the
// observations above.
// ---------------------------------------------------------------------

/**
 * @brief Point read to revalidate at commit.
 *
 * `tid` and `found` come from an earlier read or scan row. Commit
 * aborts when the row's TID moved; `found == false` asserts the key was
 * absent and aborts when a row appeared.
 */
struct ExternalReadEntry {
  std::string table_name;
  std::string key;
  uint64_t tid = 0;
  bool found = false;
};

/**
 * @brief Row write or delete to install during Commit.
 *
 * When `is_delete` is true, `value` is ignored and the row is removed.
 * When `is_insert` is true, the key must hold no live row at commit; if it
 * does, Commit aborts with @ref kDuplicateKeyAbortReason.
 */
struct ExternalWriteEntry {
  std::string table_name;
  std::string key;
  std::string value;
  bool is_delete = false;
  bool is_insert = false;
};

/// Abort reason Commit reports when an insert entry finds a live
/// row.
inline constexpr char kDuplicateKeyAbortReason[] = "duplicate_primary_key";

/// Every abort reason for a refused UNIQUE secondary key starts with this.
inline constexpr char kDuplicateSecondaryKeyAbortPrefix[] = "unique_si_";

/**
 * @brief When a commit is acknowledged, relative to its record reaching the
 * device. Carried per commit.
 *
 * The equivalent settings elsewhere, to keep Async from being read as a
 * faster Sync: Sync is PostgreSQL's synchronous_commit=on, SQL Server's full
 * durability, Oracle's COMMIT WAIT; Async is synchronous_commit=off, delayed
 * durability, COMMIT NOWAIT.
 */
enum class CommitPolicy {
  Sync,   ///< Acknowledged once the committer's own epoch is durable.
  Async,  ///< Acknowledged at precommit; a crash can lose it.
};

/**
 * @brief Secondary-index add or remove to install during Commit.
 */
struct ExternalSecondaryIndexEntry {
  std::string table_name;
  std::string index_name;
  std::string secondary_key;
  std::string primary_key;
  bool is_delete = false;
};

/**
 * @brief Range read to revalidate at commit.
 *
 * Assemble it from the scan request and the returned rows: the bounds
 * describe the scan to re-run, `result_keys` (plus `result_primary_keys`
 * on a secondary index) is the key set the scan returned, in scan order.
 * Commit replays the scan and aborts when the key set changed.
 * Row TIDs are not part of this entry; register every returned row as an
 * ExternalReadEntry instead.
 */
struct ExternalRangeReadEntry {
  std::string table_name;
  std::string index_name;     ///< Empty marks a primary-index range.
  std::string start_key;      ///< Scan start (inclusive).
  std::string end_key;        ///< Scan end (exclusive). Must be non-empty.
  uint64_t row_limit = 0;     ///< Row cap applied during scan.
  bool reverse_scan = false;  ///< Scan direction.
  std::vector<std::string> result_keys;  ///< Observed key set.
  std::vector<std::string>
      result_primary_keys;  ///< Secondary index: paired primary keys.
};

}  // namespace helios::storage

#endif  // HELIOS_STORAGE_INCLUDE_STORAGE_READ_H
