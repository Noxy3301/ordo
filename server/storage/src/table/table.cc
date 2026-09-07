#include "table/table.h"

#include <shared_mutex>
#include <string_view>
#include <tuple>
#include <utility>

#include "index/primary_index.h"
#include "storage/config.h"
#include "util/epoch_framework.h"
// #include "index/secondary_index.h"  // now included from table.h

namespace helios::storage {
Table::Table(epoch::Framework &epoch_framework, const Config &config,
             std::string_view table_name)
    : epoch_framework_(epoch_framework),
      config_(config),
      primary_index_(epoch_framework, config),
      table_name_(table_name) {}

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