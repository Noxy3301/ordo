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
 * @file server/storage/src/index/data_buffer.h
 * The row payload a DataItem owns, on the heap or in a PAX slot.
 */

#ifndef HELIOS_STORAGE_SRC_INDEX_DATA_BUFFER_H
#define HELIOS_STORAGE_SRC_INDEX_DATA_BUFFER_H

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <new>
#include <string>
#include <vector>

#include "pax/table.h"
#include "pax/version_store.h"
#include "storage/pax.h"
#include "util/spdlog.h"

namespace helios::storage {

/**
 * @brief Owns or references the row payload stored in a DataItem.
 *
 * @details Heap mode is the original layout: `value` owns a new[] byte array
 * of `size` bytes, with `capacity_or_slot` tracking the allocation. It is used
 * for transaction-local copies, non-PAX tables, and rows that fell back from
 * PAX storage.
 *
 * PAX mode: the payload bytes live in a PaxGroup's column strips; this
 * buffer only references them. `value` carries a tagged pointer
 * (bit0 = PAX, bit1 = slot allocated): before the first install it points
 * to the table's PaxTable, afterwards to the owning PaxGroup with
 * `capacity_or_slot` = slot index. `size` keeps its meaning (payload byte size,
 * 0 = tombstone/blank), so every liveness check (`size != 0`) works
 * unchanged in both modes.
 *
 * Mode transitions are one-way per row: index layers create blank items in
 * PAX mode for PAX tables; a row whose bytes don't fit the declared cell
 * widths permanently reverts to heap mode (correctness fallback).
 *
 * Copy semantics do the storage conversion implicitly: copying FROM a
 * PAX-mode buffer gathers the row into heap bytes (transaction-local
 * snapshots), assigning INTO a PAX-mode buffer scatters the bytes into the
 * strips (commit install under the row's TID lock).
 */
struct DataBuffer {
  static constexpr uintptr_t kPaxTag = 0x1;
  static constexpr uintptr_t kPaxAllocated = 0x2;
  static constexpr uintptr_t kPaxMask = kPaxTag | kPaxAllocated;

  std::byte *value;
  size_t size;
  // Heap mode: the size of the allocation. PAX mode: the slot number.
  size_t capacity_or_slot;

  DataBuffer() : value(nullptr), size(0), capacity_or_slot(0) {}
  ~DataBuffer() {
    if (value != nullptr && !is_pax()) delete[] value;
  }

  bool is_pax() const {
    return (reinterpret_cast<uintptr_t>(value) & kPaxTag) != 0;
  }
  bool pax_allocated() const {
    return (reinterpret_cast<uintptr_t>(value) & kPaxAllocated) != 0;
  }
  pax::PaxGroup *pax_group() const {
    return reinterpret_cast<pax::PaxGroup *>(
        reinterpret_cast<uintptr_t>(value) & ~kPaxMask);
  }
  pax::PaxTable *pax_table() const {
    return reinterpret_cast<pax::PaxTable *>(
        reinterpret_cast<uintptr_t>(value) & ~kPaxMask);
  }
  uint32_t pax_slot() const { return static_cast<uint32_t>(capacity_or_slot); }

  /**
   * @brief Initializes a fresh blank item as a PAX-resident row reference.
   *
   * @param store Table store that will allocate the concrete row slot on the
   * first non-empty install.
   */
  void InitPaxBlank(pax::PaxTable *store) {
    assert((reinterpret_cast<uintptr_t>(store) & kPaxMask) == 0);
    value = reinterpret_cast<std::byte *>(reinterpret_cast<uintptr_t>(store) |
                                          kPaxTag);
    size = 0;
    capacity_or_slot = 0;
  }

  DataBuffer(DataBuffer &&other) noexcept
      : value(other.value),
        size(other.size),
        capacity_or_slot(other.capacity_or_slot) {
    other.value = nullptr;
    other.size = 0;
    other.capacity_or_slot = 0;
  }

  DataBuffer &operator=(DataBuffer &&other) noexcept {
    if (this != &other) {
      if (value != nullptr && !is_pax()) delete[] value;
      value = other.value;
      size = other.size;
      capacity_or_slot = other.capacity_or_slot;
      other.value = nullptr;
      other.size = 0;
      other.capacity_or_slot = 0;
    }
    return *this;
  }

  DataBuffer(const DataBuffer &other)
      : value(nullptr), size(0), capacity_or_slot(0) {
    Reset(other);
  }

  DataBuffer &operator=(const DataBuffer &other) {
    if (this != &other) {
      Reset(other);
    }
    return *this;
  }

  // NOTE: the allocation only grows; consider shrink-to-fit if large records
  // cause bloat.
  void Reset(const std::byte *row, const size_t len) {
    if (is_pax()) {
      ResetPax(row, len);
      return;
    }
    if (row == nullptr || len == 0) {
      size = 0;
      return;
    }
    if (capacity_or_slot < len) {
      delete[] value;
      value = new std::byte[len];
      capacity_or_slot = len;
    }
    size = len;
    std::memcpy(value, row, len);
  }

  /**
   * @brief Resets this buffer to the row payload represented by `rhs`.
   */
  void Reset(const DataBuffer &rhs) {
    if (!rhs.is_pax()) {
      Reset(rhs.value, rhs.size);
      return;
    }
    if (rhs.size == 0) {
      Reset(nullptr, 0);
      return;
    }
    if (is_pax()) {
      // PAX-to-PAX copy gathers first, then scatters into this row's slot.
      thread_local std::vector<std::byte> tmp;
      tmp.resize(rhs.size);
      rhs.GatherInto(tmp.data());
      Reset(tmp.data(), rhs.size);
      return;
    }
    // Gather straight into the heap array for a transaction-local snapshot.
    if (capacity_or_slot < rhs.size) {
      delete[] value;
      value = new std::byte[rhs.size];
      capacity_or_slot = rhs.size;
    }
    size = rhs.GatherInto(value);
  }
  void Reset(const std::string &rhs) {
    Reset(reinterpret_cast<const std::byte *>(rhs.data()), rhs.size());
  }
  bool IsEmpty() const { return size == 0; }

  /**
   * @brief Reconstructs this row's payload bytes from PAX strips into `dst`.
   *
   * @param dst Destination buffer with room for `size` bytes.
   * @return Number of bytes written.
   */
  size_t GatherInto(std::byte *dst) const {
    assert(is_pax());
    assert(pax_allocated());
    assert(size > 0);
    return pax_group()->GatherRow(pax_slot(), dst, size);
  }

  std::string toString() const {
    if (size == 0) return {};  // no payload, and `value` may be null
    if (!is_pax()) return std::string(reinterpret_cast<char *>(value), size);
    std::string out;
    out.resize(size);
    GatherInto(reinterpret_cast<std::byte *>(out.data()));
    return out;
  }

 private:
  /**
   * @brief Publishes the pre-install row image while a columnar read view
   * is active.
   *
   * @details Must run before the first strip mutation of this install
   * (ScatterRow or RetireSlot). A zero commit epoch means the install is
   * outside an epoch-tagged commit (recovery replay); a read view observed
   * active in that state fails the capture for the generation, fail-closed.
   */
  void CaptureBeforeImage() {
    auto &version_store = pax::VersionStore::Global();
    if (!version_store.CaptureActive()) return;
    const uint32_t epoch = pax::CurrentCommitEpoch::Get();
    if (epoch == 0) {
      // Fail closed: skipping silently would let cells change with no
      // entry and no count advance, and the reader's end recheck would
      // pass on a torn result. The writer proceeds.
      version_store.FailCapture(
          "PAX install without a commit epoch while a read view is active");
      return;
    }
    const bool visible = size != 0;
    version_store.Capture(pax_group(), pax_slot(), epoch, visible,
                          visible ? toString() : std::string());
  }

  /**
   * @brief Installs payload bytes into this PAX-mode buffer.
   *
   * @details The caller holds the row's TID lock. If the row does not fit its
   * declared cell widths, this buffer permanently switches to heap storage.
   */
  void ResetPax(const std::byte *row, const size_t len) {
    if (row == nullptr || len == 0) {
      if (pax_allocated() && size != 0) {
        CaptureBeforeImage();
        pax_group()->RetireSlot(pax_slot());
      }
      size = 0;  // tombstone; keep the slot for the (possible) re-insert
      return;
    }
    if (!pax_allocated()) {
      auto *store = pax_table();
      auto [group, slot] = store->AllocateSlot();
      if (group == nullptr) {
        OverflowToHeap(store, "no free slot", row, len);
        return;
      }
      assert((reinterpret_cast<uintptr_t>(group) & kPaxMask) == 0);
      value = reinterpret_cast<std::byte *>(reinterpret_cast<uintptr_t>(group) |
                                            kPaxTag | kPaxAllocated);
      capacity_or_slot = slot;
    }
    CaptureBeforeImage();
    if (pax_group()->ScatterRow(pax_slot(), row, len)) {
      size = len;
      return;
    }

    // Hide the abandoned slot: strip-direct scans must not read a row this
    // buffer now keeps on the heap.
    pax_group()->RetireSlot(pax_slot());
    OverflowToHeap(pax_group()->table(), "row wider than its cell", row, len);
  }

  /**
   * @brief Moves this row to heap storage for good, and records it.
   *
   * @details A table with one such row is no longer a complete set of rows in
   * its strips, which is what the store's counter tells strip-direct readers.
   */
  void OverflowToHeap(pax::PaxTable *store, const char *why,
                      const std::byte *row, const size_t len) {
    if (store->overflow_count() == 0) {
      SPDLOG_WARN(
          "PAX overflow for table '{}': {} (row size {}); the row stays on the "
          "heap",
          store->schema().table_name, why, len);
    } else {
      SPDLOG_DEBUG("PAX overflow for table '{}': {} (row size {})",
                   store->schema().table_name, why, len);
    }
    store->RecordOverflow();
    value = nullptr;
    capacity_or_slot = 0;
    Reset(row, len);
  }
};
}  // namespace helios::storage
#endif  // HELIOS_STORAGE_SRC_INDEX_DATA_BUFFER_H
