/**
 * @file server/storage/src/table/table.cc
 * Table construction and the secondary index lookups the read path takes.
 */

#include "table/table.h"

#include <shared_mutex>
#include <string>
#include <string_view>

#include "index/primary_index.h"

namespace helios::storage {
Table::Table(std::string_view table_name) : table_name_(table_name) {}

// The lock covers the lookup only: an index is never removed, so the pointer
// stays valid after it is released.
index::SecondaryIndex *Table::GetSecondaryIndex(
    const std::string_view index_name) {
  std::shared_lock<std::shared_mutex> lk(table_lock_);
  auto it = secondary_indices_.find(std::string(index_name));
  if (it == secondary_indices_.end()) {
    return nullptr;
  }
  return it->second.get();
}

const std::string &Table::Name() const { return table_name_; }
index::PrimaryIndex &Table::GetPrimaryIndex() { return primary_index_; }

}  // namespace helios::storage