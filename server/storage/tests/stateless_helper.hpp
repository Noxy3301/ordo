#ifndef HELIOS_STATELESS_HELPER_HPP
#define HELIOS_STATELESS_HELPER_HPP

#include <lineairdb/database.h>
#include <lineairdb/stateless.h>

#include <cstring>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "gtest/gtest.h"

/// Drives the stateless API the way the query layer does: observe, then submit
/// the observations as evidence along with the writes. Every call hands the
/// thread's masstree epoch back, as an RPC handler does.
namespace TestHelper {

/// Sorts above any key a test writes, for scans that mean "to the end".
inline const std::string kMaxKey = "\xff\xff\xff\xff";

/// Raw little-endian bytes, the shape the row path stores scalars in.
template <typename T>
std::string Encode(const T &value) {
  static_assert(std::is_trivially_copyable<T>::value,
                "LineairDB stores trivially copyable types");
  std::string encoded(sizeof(T), '\0');
  std::memcpy(encoded.data(), &value, sizeof(T));
  return encoded;
}

template <typename T>
T Decode(const std::string &value) {
  T decoded{};
  std::memcpy(&decoded, value.data(), sizeof(T));
  return decoded;
}

inline bool Commit(
    LineairDB::Database &db,
    const std::vector<LineairDB::ExternalReadEntry> &reads,
    const std::vector<LineairDB::ExternalWriteEntry> &writes,
    const std::vector<LineairDB::ExternalSecondaryIndexEntry> &index_ops = {},
    const std::vector<LineairDB::ExternalRangeReadEntry> &ranges = {},
    std::string *abort_reason = nullptr) {
  const bool committed =
      db.ValidateAndCommit(reads, writes, index_ops, ranges,
                           LineairDB::CommitPolicy::Sync, abort_reason);
  db.ReleaseMasstreeThreadEpoch();
  return committed;
}

inline bool CommitWrites(
    LineairDB::Database &db,
    const std::vector<LineairDB::ExternalWriteEntry> &writes,
    const std::vector<LineairDB::ExternalSecondaryIndexEntry> &index_ops = {}) {
  return Commit(db, {}, writes, index_ops);
}

inline bool Write(LineairDB::Database &db, const std::string &table,
                  const std::string &key, const std::string &value) {
  return CommitWrites(db, {{table, key, value, false, false}});
}

template <typename T>
bool Write(LineairDB::Database &db, const std::string &table,
           const std::string &key, const T &value) {
  return Write(db, table, key, Encode<T>(value));
}

inline bool Delete(LineairDB::Database &db, const std::string &table,
                   const std::string &key) {
  return CommitWrites(db, {{table, key, "", true, false}});
}

inline std::optional<std::string> Read(LineairDB::Database &db,
                                       const std::string &table,
                                       const std::string &key) {
  auto result = db.Read(table, key);
  db.ReleaseMasstreeThreadEpoch();
  if (!result.found) return std::nullopt;
  return std::move(result.value);
}

template <typename T>
std::optional<T> Read(LineairDB::Database &db, const std::string &table,
                      const std::string &key) {
  auto value = Read(db, table, key);
  if (!value.has_value() || value->size() < sizeof(T)) return std::nullopt;
  return Decode<T>(*value);
}

/// Rows a primary-index range scan returned, in scan order.
inline std::vector<std::pair<std::string, std::string>> Scan(
    LineairDB::Database &db, const std::string &table,
    const std::string &start_key, const std::string &end_key,
    uint64_t row_limit = 0, bool reverse_scan = false) {
  auto scan = db.Scan(table, start_key, end_key, row_limit, reverse_scan);
  db.ReleaseMasstreeThreadEpoch();
  std::vector<std::pair<std::string, std::string>> rows;
  EXPECT_TRUE(scan.ok);
  if (!scan.ok) return rows;
  for (auto &row : scan.rows) {
    rows.emplace_back(std::move(row.key), std::move(row.value));
  }
  return rows;
}

/// (secondary key, primary key) pairs a secondary-index range scan returned.
inline std::vector<std::pair<std::string, std::string>> ScanSecondaryIndex(
    LineairDB::Database &db, const std::string &table,
    const std::string &index_name, const std::string &start_key,
    const std::string &end_key, uint64_t row_limit = 0,
    bool reverse_scan = false) {
  auto scan = db.ScanIndex(table, index_name, start_key, end_key, row_limit,
                           reverse_scan);
  db.ReleaseMasstreeThreadEpoch();
  std::vector<std::pair<std::string, std::string>> rows;
  EXPECT_TRUE(scan.ok);
  if (!scan.ok) return rows;
  for (auto &row : scan.rows) {
    rows.emplace_back(std::move(row.secondary_key), std::move(row.primary_key));
  }
  return rows;
}

/// Primary keys a single secondary key resolves to.
inline std::vector<std::string> ReadSecondaryIndex(
    LineairDB::Database &db, const std::string &table,
    const std::string &index_name, const std::string &secondary_key) {
  std::vector<std::string> primary_keys;
  for (auto &entry : ScanSecondaryIndex(db, table, index_name, secondary_key,
                                        secondary_key + '\0')) {
    primary_keys.push_back(std::move(entry.second));
  }
  return primary_keys;
}

}  // namespace TestHelper
#endif /* HELIOS_STATELESS_HELPER_HPP */
