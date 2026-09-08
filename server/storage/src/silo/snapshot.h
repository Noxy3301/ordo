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

/**
 * @file server/storage/src/silo/snapshot.h
 * One entry of a transaction's write set, and what it does to the
 * secondary indexes.
 */

#ifndef HELIOS_STORAGE_SRC_SILO_SNAPSHOT_H
#define HELIOS_STORAGE_SRC_SILO_SNAPSHOT_H

#include <string>
#include <string_view>
#include <vector>

#include "index/data_item.h"
#include "index/index_constraint.h"
#include "util/epoch.h"

namespace helios::storage {

enum class SecondaryIndexOp : uint8_t {
  None = 0,
  Add = 1,
  Remove = 2,
  Full = 3,
};

struct Snapshot {
  std::string key;
  DataItem data_item_copy;
  DataItem *index_cache;
  std::string table_name;
  std::string index_name;
  index::IndexConstraint index_type;
  struct SecondaryIndexDelta {
    std::string primary_key;
    SecondaryIndexOp op;
  };
  std::vector<SecondaryIndexDelta> secondary_index_deltas;

  Snapshot(const std::string_view k, const std::byte v[], const size_t s,
           DataItem *const i, std::string_view tn, std::string_view in,
           const TransactionId ver = {},
           index::IndexConstraint it = index::IndexConstraint())
      : key(k), index_cache(i), table_name(tn), index_name(in), index_type(it) {
    if (v != nullptr) data_item_copy.Reset(v, s, ver);
  }
  Snapshot(const Snapshot &) = default;
  Snapshot &operator=(const Snapshot &) = default;
  // Declaring copy ctor/assign above suppresses implicit move generation;
  // an emplace_back(std::move(snapshot)) would otherwise silently fall back
  // to the deep copy path
  Snapshot(Snapshot &&) = default;
  Snapshot &operator=(Snapshot &&) = default;

  void RecordIndexDelta(const std::string_view primary_key,
                        SecondaryIndexOp op) {
    for (auto &delta : secondary_index_deltas) {
      if (delta.primary_key == primary_key) {
        delta.op = op;
        return;
      }
    }
    secondary_index_deltas.push_back({std::string(primary_key), op});
  }
};

using WriteSetType = std::vector<Snapshot>;

}  // namespace helios::storage

#endif  // HELIOS_STORAGE_SRC_SILO_SNAPSHOT_H
