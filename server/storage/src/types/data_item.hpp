/*
 *   Copyright (c) 2020 Nippon Telegraph and Telephone Corporation
 *   All rights reserved.

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

#ifndef HELIOS_DATA_ITEM_HPP
#define HELIOS_DATA_ITEM_HPP

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstring>
#include <memory>
#include <msgpack.hpp>
#include <string>
#include <type_traits>
#include <vector>

#include "data_buffer.hpp"
#include "packed_primary_keys.hpp"
#include "types/transaction_id.hpp"

namespace helios::storage {

struct DataItem {
  std::atomic<TransactionId> transaction_id;
  DataBuffer buffer;
  std::shared_ptr<const PackedPrimaryKeys> primary_keys_;

  // Direct byte access is invalid for PAX-resident rows (no contiguous
  // bytes); those callers must go through DataBuffer::GatherInto / copies.
  std::byte *value() {
    assert(!buffer.is_pax());
    return &buffer.value[0];
  }
  const std::byte *value() const {
    assert(!buffer.is_pax());
    return &buffer.value[0];
  }
  size_t size() const { return buffer.size; }
  bool IsInitialized() const {
    if (buffer.size != 0) return true;
    const auto primary_keys = std::atomic_load(&primary_keys_);
    return primary_keys && primary_keys->count != 0;
  }
  bool IsPrimaryInitialized() const { return buffer.size != 0; }

  PackedPrimaryKeysView primary_keys_view() const {
    return PackedPrimaryKeysView(primary_keys_);
  }

  std::vector<std::string> primary_keys_vector() const {
    std::vector<std::string> keys;
    const auto view = primary_keys_view();
    keys.reserve(view.size());
    for (std::string_view key : view) {
      keys.emplace_back(key.data(), key.size());
    }
    return keys;
  }

  void SetPrimaryKeys(const std::vector<std::string> &primary_keys) {
    assert(IsSortedDeduped(primary_keys));
    auto packed = PackedPrimaryKeys::FromSortedDeduped(primary_keys);
    std::atomic_store(&primary_keys_, std::move(packed));
  }
  void SetPrimaryKeys(std::vector<std::string> &&primary_keys) {
    assert(IsSortedDeduped(primary_keys));
    auto packed = PackedPrimaryKeys::FromSortedDeduped(primary_keys);
    std::atomic_store(&primary_keys_, std::move(packed));
  }

  DataItem() : transaction_id(0) {}
  DataItem(const DataItem &rhs)
      : transaction_id(rhs.transaction_id.load()),
        primary_keys_(std::atomic_load(&rhs.primary_keys_)) {
    buffer.Reset(rhs.buffer);
    /* if (rhs.sec_idx_buffers) {
      sec_idx_buffers =
          std::make_unique<std::vector<DataBuffer>>(*rhs.sec_idx_buffers);
    } */
  }

  DataItem &operator=(const DataItem &rhs) {
    transaction_id.store(rhs.transaction_id.load());
    buffer.Reset(rhs.buffer);

    /* if (rhs.sec_idx_buffers) {
      sec_idx_buffers =
          std::make_unique<std::vector<DataBuffer>>(*rhs.sec_idx_buffers);
    } else {
      sec_idx_buffers = nullptr;
    } */
    auto primary_keys = std::atomic_load(&rhs.primary_keys_);
    std::atomic_store(&primary_keys_, std::move(primary_keys));
    return *this;
  }

  DataItem(DataItem &&rhs) noexcept
      : transaction_id(rhs.transaction_id.load()),
        buffer(std::move(rhs.buffer)),
        primary_keys_(std::move(rhs.primary_keys_)) {}

  DataItem &operator=(DataItem &&rhs) noexcept {
    transaction_id.store(rhs.transaction_id.load());
    buffer = std::move(rhs.buffer);
    std::atomic_store(&primary_keys_, std::move(rhs.primary_keys_));
    return *this;
  }

  void Reset(const std::byte *v, const size_t s, TransactionId tid = 0) {
    buffer.Reset(v, s);
    if (!tid.IsEmpty()) transaction_id.store(tid);
  }

  void AddSecondaryIndexValue(const std::byte *v, size_t s) {
    std::string_view new_key(reinterpret_cast<const char *>(v), s);
    auto current = std::atomic_load(&primary_keys_);
    auto next = PackedPrimaryKeys::Insert(current, new_key);
    if (next != current) {
      std::atomic_store(&primary_keys_, std::move(next));
    }
  }

  void RemoveSecondaryIndexValue(const std::byte *v, size_t s) {
    std::string_view target(reinterpret_cast<const char *>(v), s);
    auto current = std::atomic_load(&primary_keys_);
    auto next = PackedPrimaryKeys::Erase(current, target);
    if (next != current) {
      std::atomic_store(&primary_keys_, std::move(next));
    }
  }

 private:
  static bool IsSortedDeduped(const std::vector<std::string> &keys) {
    return std::adjacent_find(
               keys.begin(), keys.end(),
               [](const std::string &lhs, const std::string &rhs) {
                 return !(lhs < rhs);
               }) == keys.end();
  }
};

static_assert(sizeof(DataItem) == 48,
              "DataItem must remain 48 bytes in the slim layout");
}  // namespace helios::storage
#endif /* HELIOS_DATA_ITEM_HPP */
