#ifndef HELIOS_STORAGE_SRC_INDEX_SECONDARY_INDEX_H
#define HELIOS_STORAGE_SRC_INDEX_SECONDARY_INDEX_H

#include "index/index_constraint.h"
#include "index/masstree_index.h"
#include "silo/snapshot.h"
#include "silo/stable_read.h"
#include "util/epoch_framework.h"

namespace helios::storage {
namespace index {

class SecondaryIndex {
 public:
  SecondaryIndex(epoch::Framework &epoch_framework, Config config = Config(),
                 IndexConstraint index_type = IndexConstraint(),
                 [[maybe_unused]] WriteSetType recovery_set = WriteSetType())
      : index_type_(index_type), secondary_index_(config, epoch_framework) {}

  DataItem *Get(std::string_view key) { return secondary_index_.Get(key); }

  DataItem *GetOrInsert(std::string_view key) {
    auto *item = secondary_index_.Get(key);
    if (item == nullptr) {
      secondary_index_.PutBlank(key);
      item = secondary_index_.Get(key);
      assert(item != nullptr);
    }
    return item;
  }

  DataItem *GetOrInsertForWrite(std::string_view key) {
    // OCC guards existing entries. A key whose slot carries no live PK list
    // still needs a blank entry the write can fill in.
    auto *item = secondary_index_.Get(key);
    if (item == nullptr || !silo::StableReadKeys(*item).found) {
      secondary_index_.PutBlank(key);
      item = secondary_index_.Get(key);
      assert(item != nullptr);
    }
    return item;
  }

  size_t Scan(std::string_view begin, std::optional<std::string_view> end,
              std::function<bool(std::string_view)> operation) {
    return secondary_index_.Scan(begin, end, operation);
  }

  size_t ScanReverse(std::string_view begin,
                     std::optional<std::string_view> end,
                     std::function<bool(std::string_view)> operation) {
    return secondary_index_.ScanReverse(begin, end, operation);
  }

  bool Purge(std::string_view key, DataItem *expected,
             TransactionId retired_tid = {}) {
    return secondary_index_.Purge(key, expected, retired_tid);
  }

  void ForEach(std::function<bool(std::string_view, DataItem &)> f) {
    secondary_index_.ForEach(f);
  }

  bool Put(const std::string_view key, DataItem &&value) {
    return secondary_index_.Put(key, std::forward<DataItem>(value));
  }

  bool IsUnique() { return index_type_.IsUnique(); }

  IndexConstraint GetIndexType() const { return index_type_; }

 private:
  IndexConstraint index_type_;
  MasstreeIndex secondary_index_;
};
}  // namespace index
}  // namespace helios::storage

#endif  // HELIOS_STORAGE_SRC_INDEX_SECONDARY_INDEX_H
