/**
 * @file server/storage/src/silo/read.h
 * The read side of the store: point reads and range scans. Each call takes a
 * shared lock on the schema, copies rows with the Silo-style stable read, and
 * returns the packed TIDs a later commit validates as read-set evidence.
 */

#ifndef HELIOS_STORAGE_SRC_SILO_READ_H
#define HELIOS_STORAGE_SRC_SILO_READ_H

#include <cstdint>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "storage/read.h"

namespace helios::storage {

class TableDictionary;

namespace silo {

/**
 * @brief Reads one row.
 *
 * Takes a shared lock on the schema, resolves the primary-index slot, and
 * performs a Silo-style double TID read on the DataItem: load the TID,
 * yield while the lock bit (LSB) is set, copy the value, then re-load the
 * TID and only return it if it has not moved. The caller keeps the
 * returned `tid` and submits it through Commit later.
 */
ReadResult Read(TableDictionary &tables, std::shared_mutex &schema_mutex,
                std::string_view table_name, std::string_view key,
                const std::vector<uint32_t> *selected_columns = nullptr);

/**
 * @brief Reads several rows in one call.
 *
 * Reuses the point read per entry. Reads are independent, so this is purely a
 * transport optimization that lets a caller fold N point reads into one
 * RPC.
 */
std::vector<ReadResult> BatchRead(
    TableDictionary &tables, std::shared_mutex &schema_mutex,
    const std::vector<std::pair<std::string, std::string>> &keys);

/**
 * @brief Range-scans the primary index and returns the rows observed in
 *        the range.
 *
 * Drives index::Scan / index::ScanReverse with a callback that, for each
 * hit, performs the same double-TID read used by the point read. The caller
 * assembles the commit-time ExternalRangeReadEntry from its own scan
 * arguments and the returned keys. Tombstones are skipped: key-list
 * validation catches any reuse of their slots without a per-entry TID.
 *
 * `ok` distinguishes a genuine empty result from a scan that never ran:
 * the table does not exist, or `end_key` is empty. Callers should treat
 * `!ok` as an abort signal.
 */
ScanResult Scan(TableDictionary &tables, std::shared_mutex &schema_mutex,
                std::string_view table_name, std::string_view start_key,
                std::string_view end_key, uint64_t row_limit, bool reverse_scan,
                const std::vector<uint32_t> *selected_columns = nullptr);

/**
 * @brief Range-scans a secondary index and resolves each hit to its base
 *        row.
 *
 * For every secondary key in `[start_key, end_key)`, pins its immutable
 * primary-key list and, for each key in the view, performs the same double-TID
 * base read as the point read. The caller assembles the commit-time
 * ExternalRangeReadEntry from its own scan arguments and both returned
 * key lists. `ok == false` is the abort signal, as in the primary range read.
 */
ScanIndexResult ScanIndex(
    TableDictionary &tables, std::shared_mutex &schema_mutex,
    std::string_view table_name, std::string_view index_name,
    std::string_view start_key, std::string_view end_key, uint64_t row_limit,
    bool reverse_scan, const std::vector<uint32_t> *selected_columns = nullptr);

/**
 * @brief Range-scans the primary index and returns PAX cell references.
 *
 * @details The caller evaluates cells directly and re-checks each row TID
 * after reading. `ok == false` means the caller should use the row-shaped
 * range read instead.
 */
ScanPaxResult ScanPax(TableDictionary &tables, std::shared_mutex &schema_mutex,
                      std::string_view table_name, std::string_view start_key,
                      std::string_view end_key, uint64_t row_limit,
                      bool reverse_scan);

}  // namespace silo
}  // namespace helios::storage

#endif  // HELIOS_STORAGE_SRC_SILO_READ_H
