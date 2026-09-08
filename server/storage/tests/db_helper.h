/**
 * @file server/storage/tests/db_helper.h
 * Test fixtures that drive the read and commit API the way a request does:
 * observe, then submit the observations as evidence with the writes.
 */

#ifndef HELIOS_STORAGE_TESTS_DB_HELPER_H
#define HELIOS_STORAGE_TESTS_DB_HELPER_H

#include <cstring>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "storage/commit.h"
#include "storage/database.h"
#include "storage/read.h"

// Drives the read and commit API the way the query layer does: observe, then
// submit the observations as evidence along with the writes. Every call hands
// the thread's masstree epoch back, as an RPC handler does.
namespace TestHelper {

// Sorts above any key a test writes, for scans that mean "to the end".
inline const std::string kMaxKey = "\xff\xff\xff\xff";

// The host bytes of a scalar, as a test row payload.
template <typename T>
std::string Pack(const T &value) {
  static_assert(std::is_trivially_copyable<T>::value,
                "Helios stores trivially copyable types");
  std::string buf(sizeof(T), '\0');
  std::memcpy(buf.data(), &value, sizeof(T));
  return buf;
}

template <typename T>
T Unpack(const std::string &value) {
  T buf{};
  std::memcpy(&buf, value.data(), sizeof(T));
  return buf;
}

inline bool Commit(
    helios::storage::Database &db,
    const std::vector<helios::storage::ExternalReadEntry> &reads,
    const std::vector<helios::storage::ExternalWriteEntry> &writes,
    const std::vector<helios::storage::ExternalSecondaryIndexEntry> &index_ops =
        {},
    const std::vector<helios::storage::ExternalRangeReadEntry> &ranges = {},
    std::string *abort_reason = nullptr) {
  const bool committed =
      db.Commit(reads, writes, index_ops, ranges,
                helios::storage::CommitDurability::kSync, abort_reason);
  db.ReleaseThreadEpoch();
  return committed;
}

inline bool CommitWrites(
    helios::storage::Database &db,
    const std::vector<helios::storage::ExternalWriteEntry> &writes,
    const std::vector<helios::storage::ExternalSecondaryIndexEntry> &index_ops =
        {}) {
  return Commit(db, {}, writes, index_ops);
}

inline bool Write(helios::storage::Database &db, const std::string &table,
                  const std::string &key, const std::string &value) {
  return CommitWrites(db, {{table, key, value}});
}

template <typename T>
bool Write(helios::storage::Database &db, const std::string &table,
           const std::string &key, const T &value) {
  return Write(db, table, key, Pack<T>(value));
}

inline bool Delete(helios::storage::Database &db, const std::string &table,
                   const std::string &key) {
  return CommitWrites(db, {{table, key, "", helios::storage::RowOp::kDelete}});
}

inline std::optional<std::string> Read(helios::storage::Database &db,
                                       const std::string &table,
                                       const std::string &key) {
  auto result = db.Read(table, key);
  db.ReleaseThreadEpoch();
  if (!result.found) return std::nullopt;
  return std::move(result.value);
}

template <typename T>
std::optional<T> Read(helios::storage::Database &db, const std::string &table,
                      const std::string &key) {
  auto value = Read(db, table, key);
  if (!value.has_value() || value->size() < sizeof(T)) return std::nullopt;
  return Unpack<T>(*value);
}

// Rows a primary-index range scan returned, in scan order.
inline std::vector<std::pair<std::string, std::string>> Scan(
    helios::storage::Database &db, const std::string &table,
    const std::string &start_key, const std::string &end_key,
    uint64_t row_limit = 0, bool reverse_scan = false) {
  auto scan = db.Scan(table, start_key, end_key, row_limit, reverse_scan);
  db.ReleaseThreadEpoch();
  std::vector<std::pair<std::string, std::string>> rows;
  EXPECT_TRUE(scan.ok);
  if (!scan.ok) return rows;
  for (auto &row : scan.rows) {
    rows.emplace_back(std::move(row.key), std::move(row.value));
  }
  return rows;
}

// (secondary key, primary key) pairs a secondary-index range scan returned.
inline std::vector<std::pair<std::string, std::string>> ScanIndex(
    helios::storage::Database &db, const std::string &table,
    const std::string &index_name, const std::string &start_key,
    const std::string &end_key, uint64_t row_limit = 0,
    bool reverse_scan = false) {
  auto scan = db.ScanIndex(table, index_name, start_key, end_key, row_limit,
                           reverse_scan);
  db.ReleaseThreadEpoch();
  std::vector<std::pair<std::string, std::string>> rows;
  EXPECT_TRUE(scan.ok);
  if (!scan.ok) return rows;
  for (auto &row : scan.rows) {
    rows.emplace_back(std::move(row.secondary_key), std::move(row.primary_key));
  }
  return rows;
}

// Primary keys a single secondary key resolves to.
inline std::vector<std::string> ReadIndex(helios::storage::Database &db,
                                          const std::string &table,
                                          const std::string &index_name,
                                          const std::string &secondary_key) {
  std::vector<std::string> primary_keys;
  // The exclusive end of an exact-match scan of one secondary key.
  for (auto &entry :
       ScanIndex(db, table, index_name, secondary_key, secondary_key + '\0')) {
    primary_keys.push_back(std::move(entry.second));
  }
  return primary_keys;
}

}  // namespace TestHelper
#endif  // HELIOS_STORAGE_TESTS_DB_HELPER_H
