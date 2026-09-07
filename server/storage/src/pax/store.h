/** @file server/storage/src/pax/store.h
 * The append-only group directory behind one table's PAX strips.
 */

#ifndef HELIOS_STORAGE_SRC_PAX_STORE_H
#define HELIOS_STORAGE_SRC_PAX_STORE_H

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <utility>

#include "storage/pax.h"

namespace helios::storage {
namespace pax {

/**
 * @brief Owns all PAX row groups for one Helios table.
 *
 * @details `PaxStore` assigns append-only `(group, slot)` locations. It does
 * not publish rows to indexes and does not decide transaction visibility; those
 * remain in the existing Helios `DataItem`, Silo, and Masstree layers.
 */
class PaxStore {
 public:
  // 262,144 groups x 8,192 rows = 2^31 slots per table.
  static constexpr size_t kMaxGroups = 1u << 18;

  /**
   * @brief Takes ownership of the table schema used by subsequently allocated
   * groups.
   *
   * @param schema Schema copied into the store and referenced by its groups.
   */
  explicit PaxStore(TableSchema schema);

  /**
   * @brief Allocates the next append-only PAX slot.
   *
   * @details Slots are append-only and are not reused.
   *
   * @return `{nullptr, 0}` when the table has exhausted the fixed directory, so
   * the caller can fall back to heap row storage without losing correctness.
   */
  std::pair<PaxGroup *, uint32_t> AllocateSlot();

  /**
   * @brief Returns the schema used to size every group in this store.
   */
  const TableSchema &schema() const { return schema_; }

  /**
   * @brief Returns group `idx`, or nullptr if it has not been allocated yet.
   *
   * @param idx Group index in the append-only directory.
   */
  PaxGroup *group(size_t idx) const {
    return dir_[idx].load(std::memory_order_acquire);
  }

  /**
   * @brief Returns slots handed out, an upper bound on populated rows.
   */
  uint64_t slots_allocated() const {
    return next_slot_.load(std::memory_order_acquire);
  }

  /**
   * @brief Returns the number of row groups that may contain allocated slots.
   */
  size_t group_count() const {
    const uint64_t slots = slots_allocated();
    return static_cast<size_t>((slots + PaxGroup::kRows - 1) / PaxGroup::kRows);
  }

  /**
   * @brief Records one row that used heap fallback instead of PAX cells.
   */
  void RecordHeapFallback() {
    overflow_count_.fetch_add(1, std::memory_order_relaxed);
  }

  /**
   * @brief Returns the number of rows that used heap fallback.
   */
  uint64_t overflow_count() const {
    return overflow_count_.load(std::memory_order_relaxed);
  }

 private:
  TableSchema schema_;
  std::unique_ptr<std::atomic<PaxGroup *>[]> dir_;
  std::atomic<uint64_t> next_slot_{0};
  std::atomic<uint64_t> overflow_count_{0};
  std::mutex grow_mutex_;
};

}  // namespace pax
}  // namespace helios::storage

#endif  // HELIOS_STORAGE_SRC_PAX_STORE_H
