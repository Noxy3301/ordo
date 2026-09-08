/**
 * @file server/storage/src/table/table.h
 * One table: its primary index, its secondary indexes, and the lock that
 * keeps a definition change apart from the reads.
 */

#ifndef HELIOS_STORAGE_SRC_TABLE_TABLE_H
#define HELIOS_STORAGE_SRC_TABLE_TABLE_H

#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>

#include "index/primary_index.h"
#include "index/secondary_index.h"
#include "pax/store.h"
#include "storage/pax.h"

namespace helios::storage {

class Table {
 public:
  explicit Table(std::string_view table_name);

  /**
   * @brief Creates a secondary index of that name.
   *
   * @return false when an index of that name already exists.
   */
  bool CreateSecondaryIndex(const std::string_view index_name,
                            const index::IndexConstraint index_type) {
    if (GetSecondaryIndex(index_name) != nullptr) return false;
    return GetOrCreateSecondaryIndex(index_name, index_type) != nullptr;
  }

  /**
   * @brief Installs PAX storage metadata for rows created after the call.
   *
   * @details The schema records per-field maximum cell widths. Existing rows
   * remain on the heap-backed DataBuffer layout; future rows are initialized
   * with the table's PaxStore. A table accepts only one PAX schema.
   *
   * @return true when the schema is installed for this table.
   * @return false when a schema has already been installed.
   */
  bool InstallPaxSchema(pax::TableSchema schema) {
    std::unique_lock<std::shared_mutex> lk(table_lock_);
    if (pax_store_ != nullptr) return false;
    pax_store_ = std::make_unique<pax::PaxStore>(std::move(schema));
    primary_index_.SetPaxStore(pax_store_.get());
    return true;
  }

  /**
   * @brief Returns the table's PAX store, or nullptr when no PAX schema has
   * been installed on it.
   */
  pax::PaxStore *GetPaxStore() const {
    std::shared_lock<std::shared_mutex> lk(table_lock_);
    return pax_store_.get();
  }

  const std::string &Name() const;

  index::PrimaryIndex &GetPrimaryIndex();

  /** The index of that name, or nullptr. */
  index::SecondaryIndex *GetSecondaryIndex(const std::string_view index_name);

  /**
   * @brief Calls `f(name, index)` for each secondary index of this table.
   *
   * @details Runs under the table lock, so `f` must not call back into a
   * method that changes the table's definition.
   */
  template <typename Func>
  void ForEachSecondaryIndex(Func &&f) {
    std::shared_lock<std::shared_mutex> lk(table_lock_);
    for (auto &[index_name, index_ptr] : secondary_indices_) {
      f(index_name, *index_ptr);
    }
  }

  /**
   * @brief Returns the index of that name, creating it when there is none.
   *
   * @return The index, or nullptr when an index of that name is declared with
   * a different constraint.
   */
  index::SecondaryIndex *GetOrCreateSecondaryIndex(
      const std::string_view index_name,
      const index::IndexConstraint index_type) {
    std::unique_lock<std::shared_mutex> lk(table_lock_);
    auto it = secondary_indices_.find(std::string(index_name));
    if (it != secondary_indices_.end()) {
      const bool same = it->second->GetIndexType().Raw() == index_type.Raw();
      return same ? it->second.get() : nullptr;
    }
    auto new_index = std::make_unique<index::SecondaryIndex>(index_type);
    auto *created = new_index.get();
    secondary_indices_[std::string(index_name)] = std::move(new_index);
    return created;
  }

 private:
  index::PrimaryIndex primary_index_;
  std::unique_ptr<pax::PaxStore> pax_store_;
  mutable std::shared_mutex table_lock_;
  std::unordered_map<std::string, std::unique_ptr<index::SecondaryIndex>>
      secondary_indices_;
  std::string table_name_;
};
}  // namespace helios::storage

#endif  // HELIOS_STORAGE_SRC_TABLE_TABLE_H
