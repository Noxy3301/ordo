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

/**
 * @file server/storage/src/index/primary_index.cc
 * The primary index: the key to DataItem map every base row lives in.
 */

#include "index/primary_index.h"

#include <functional>

#include "index/data_item.h"
#include "index/masstree_index.h"
#include "storage/config.h"
#include "util/epoch.h"

namespace helios::storage {
namespace index {

PrimaryIndex::PrimaryIndex(epoch::Framework &epoch_framework, Config config,
                           WriteSetType recovery_set)
    : index_(config, epoch_framework), epoch_manager_ref_(epoch_framework) {
  if (recovery_set.empty()) return;
  for (auto &entry : recovery_set) {
    index_.Put(entry.key, DataItem(*entry.index_cache));
  }
}

DataItem *PrimaryIndex::Get(const std::string_view key) {
  return index_.Get(key);
}

DataItem *PrimaryIndex::GetOrInsert(const std::string_view key) {
  auto *item = index_.Get(key);
  if (item == nullptr) {
    index_.PutBlank(key);
    item = index_.Get(key);
    assert(item != nullptr);
  }
  return item;
}

// return false if a corresponding entry already exists
void PrimaryIndex::Put(const std::string_view key, DataItem &&value) {
  index_.Put(key, std::move(value));
}

void PrimaryIndex::ForEach(
    std::function<bool(std::string_view, DataItem &)> f) {
  index_.ForEach(f);
}

size_t PrimaryIndex::Scan(const std::string_view begin,
                          const std::optional<std::string_view> end,
                          std::function<bool(std::string_view)> operation) {
  return index_.Scan(begin, end, operation);
}

size_t PrimaryIndex::Scan(
    const std::string_view begin, const std::string_view end,
    std::function<bool(std::string_view, DataItem &)> operation) {
  return index_.Scan(begin, end, operation);
}

size_t PrimaryIndex::ScanReverse(
    const std::string_view begin, const std::optional<std::string_view> end,
    std::function<bool(std::string_view)> operation) {
  return index_.ScanReverse(begin, end, operation);
}

size_t PrimaryIndex::ScanReverse(
    const std::string_view begin, const std::string_view end,
    std::function<bool(std::string_view, DataItem &)> operation) {
  return index_.ScanReverse(begin, end, operation);
}

}  // namespace index
}  // namespace helios::storage
