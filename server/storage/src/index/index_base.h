#ifndef LINEAIRDB_INDEX_BASE_H
#define LINEAIRDB_INDEX_BASE_H

#include <lineairdb/pax_store.h>

#include <cstdint>
#include <functional>
#include <optional>
#include <string_view>
#include <vector>

#include "types/data_item.hpp"

namespace LineairDB {
namespace Index {

class IndexBase;

// Opaque token for deferred phantom detection in tree-structured indexes
// (Masstree). Each entry pairs a backend-private node handle with the
// version observed at scan time; the owning IndexBase re-reads and compares
// at Precommit. PL ignores these entries because it detects phantoms
// synchronously at scan time.
struct NodeVersionEntry {
  IndexBase *owner;
  const void *node_ptr;
  std::uint64_t version;
};

// Reports what an Insert/Put/ForcePutBlankEntry actually did at the leaf
// version level. Used by OCC backends to apply the Silo §4.6 self-bump rule:
// if the affected leaf is already in the transaction's node-set with
// `old_version`, advance that entry to `new_version` (and only abort the tx
// if some other concurrent writer raced between the scan and our insert).
// `valid=false` means the call did not bump any leaf version (e.g. an
// in-place overwrite, or the call was a no-op).
struct NodeVersionUpdate {
  IndexBase *owner = nullptr;
  const void *node_ptr = nullptr;
  std::uint64_t old_version = 0;
  std::uint64_t new_version = 0;
  bool valid = false;
};

class IndexBase {
 public:
  virtual ~IndexBase() = default;

  /**
   * @brief Routes future blank primary rows through a table PAX store.
   *
   * @details Backends without PAX support ignore this hook, so their rows keep
   * the ordinary heap-backed DataBuffer layout. Secondary indexes never set a
   * PaxStore because they store index metadata rather than table row payloads.
   */
  virtual void SetPaxStore(Pax::PaxStore * /*store*/) {}

  // Point operations. The optional `out_update` lets the OCC layer learn
  // whether the call structurally bumped a leaf version, so it can apply
  // the Silo §4.6 own-write node-set rule (advance any matching node-set
  // entry from old_version to new_version, abort only on a real race).
  // Backends that do not track leaf versions (PL) leave `out_update->valid`
  // false.
  virtual DataItem *Get(std::string_view key) = 0;
  virtual bool Put(std::string_view key, DataItem &&rhs,
                   NodeVersionUpdate *out_update = nullptr) = 0;
  virtual bool Insert(std::string_view key,
                      NodeVersionUpdate *out_update = nullptr) = 0;
  virtual bool Delete(std::string_view key) = 0;

  // Seed a blank entry for a key that later writes fill in. Idempotent on an
  // existing key.
  virtual void ForcePutBlankEntry(std::string_view key,
                                  NodeVersionUpdate *out_update = nullptr) = 0;

  // Range operations. Returns the number of keys the walk emitted; backends
  // that defer phantom checks append per-scan snapshots into `out_versions`
  // for later ValidatePhantoms.
  virtual size_t Scan(
      std::string_view begin, std::optional<std::string_view> end,
      std::function<bool(std::string_view)> operation,
      std::vector<NodeVersionEntry> *out_versions = nullptr) = 0;
  virtual size_t Scan(
      std::string_view begin, std::string_view end,
      std::function<bool(std::string_view, DataItem &)> operation,
      std::vector<NodeVersionEntry> *out_versions = nullptr) = 0;
  virtual size_t ScanReverse(
      std::string_view begin, std::optional<std::string_view> end,
      std::function<bool(std::string_view)> operation,
      std::vector<NodeVersionEntry> *out_versions = nullptr) = 0;
  virtual size_t ScanReverse(
      std::string_view begin, std::string_view end,
      std::function<bool(std::string_view, DataItem &)> operation,
      std::vector<NodeVersionEntry> *out_versions = nullptr) = 0;

  virtual void ForEach(
      std::function<bool(std::string_view, DataItem &)> operation) = 0;

  virtual void WaitForIndexIsLinearizable() = 0;

  // Re-check every entry in `entries` whose owner is `this`. Returns true if
  // no phantom has been observed since the scan that produced them. Entries
  // owned by other indexes must be ignored (kept in the combined set so the
  // caller can validate many indexes in one pass).
  virtual bool ValidatePhantoms(
      const std::vector<NodeVersionEntry> &entries) = 0;

  // Structurally remove a committed tombstone from the index. Called by the
  // deferred purge reaper only, after it has locked `expected`, verified the
  // delete TID, and confirmed the key still resolves to the same DataItem.
  // `retired_tid` is published on the removed item before it is RCU-retired.
  // Default no-op for backends without physical reclamation (PL).
  virtual bool Purge(std::string_view /*key*/, DataItem * /*expected*/,
                     TransactionId /*retired_tid*/ = {}) {
    return false;
  }
};

}  // namespace Index
}  // namespace LineairDB

#endif /* LINEAIRDB_INDEX_BASE_H */
