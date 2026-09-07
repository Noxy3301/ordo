#include "silo/read.h"

#include <mutex>

#include "index/secondary_index.h"
#include "silo/packed_transaction_id.h"
#include "silo/stable_read.h"
#include "table/table.h"
#include "table/table_dictionary.h"
#include "types/data_item.h"

namespace helios::storage {
namespace silo {

ReadResult Read(TableDictionary &tables, std::shared_mutex &schema_mutex,
                const std::string_view table_name, const std::string_view key,
                const std::vector<uint32_t> *selected_columns) {
  std::shared_lock<std::shared_mutex> lk(schema_mutex);
  auto table = tables.GetTable(table_name);
  if (!table.has_value()) return {};

  DataItem *item = table.value()->GetPrimaryIndex().Get(key);
  if (item == nullptr) return {};

  auto row = selected_columns != nullptr
                 ? StableReadValueMasked(*item, selected_columns->data(),
                                         selected_columns->size())
                 : StableReadValue(*item);
  return {row.found, std::move(row.value), PackTransactionId(row.tid)};
}

std::vector<ReadResult> BatchRead(
    TableDictionary &tables, std::shared_mutex &schema_mutex,
    const std::vector<std::pair<std::string, std::string>> &keys) {
  std::vector<ReadResult> results;
  results.reserve(keys.size());
  for (const auto &key : keys) {
    results.emplace_back(Read(tables, schema_mutex, key.first, key.second));
  }
  return results;
}

ScanResult Scan(TableDictionary &tables, std::shared_mutex &schema_mutex,
                const std::string_view table_name,
                const std::string_view start_key,
                const std::string_view end_key, uint64_t row_limit,
                bool reverse_scan,
                const std::vector<uint32_t> *selected_columns) {
  ScanResult result;
  if (end_key.empty()) return result;

  std::shared_lock<std::shared_mutex> lk(schema_mutex);
  auto table = tables.GetTable(table_name);
  if (!table.has_value()) return result;
  result.ok = true;

  uint64_t returned_rows = 0;

  // The value-yielding Scan/ScanReverse overloads pass the DataItem the leaf
  // walk already resolved, so read it directly instead of re-fetching by key.
  auto append_scan_entry = [&](std::string_view key, DataItem &item_ref) {
    auto row = selected_columns != nullptr
                   ? StableReadValueMasked(item_ref, selected_columns->data(),
                                           selected_columns->size())
                   : StableReadValue(item_ref);
    if (row.found) {
      result.rows.push_back({std::string(key), std::move(row.value),
                             PackTransactionId(row.tid), true});
      ++returned_rows;
    }
    // Tombstones are skipped: Purge erases them at commit, and key-list
    // validation catches any reuse without needing a per-entry TID.
    return row_limit > 0 && returned_rows >= row_limit;
  };

  if (reverse_scan) {
    table.value()->GetPrimaryIndex().ScanReverse(start_key, end_key,
                                                 append_scan_entry);
  } else {
    table.value()->GetPrimaryIndex().Scan(start_key, end_key,
                                          append_scan_entry);
  }
  return result;
}

}  // namespace silo

uint64_t CurrentTid(const ScanPaxRow &row) {
  const auto *item = static_cast<const DataItem *>(row.item);
  return silo::PackTransactionId(item->transaction_id.load());
}

namespace silo {

ScanIndexResult ScanIndex(TableDictionary &tables,
                          std::shared_mutex &schema_mutex,
                          const std::string_view table_name,
                          const std::string_view index_name,
                          const std::string_view start_key,
                          const std::string_view end_key, uint64_t row_limit,
                          bool reverse_scan,
                          const std::vector<uint32_t> *selected_columns) {
  ScanIndexResult result;
  if (end_key.empty()) return result;

  std::shared_lock<std::shared_mutex> lk(schema_mutex);
  auto table = tables.GetTable(table_name);
  if (!table.has_value()) return result;

  index::SecondaryIndex *index = table.value()->GetSecondaryIndex(index_name);
  if (index == nullptr) return result;
  result.ok = true;

  uint64_t returned_rows = 0;

  auto append_base_row = [&](std::string_view secondary_key,
                             std::string_view primary_key) {
    DataItem *item = table.value()->GetPrimaryIndex().Get(primary_key);
    if (item == nullptr) {
      return false;
    }

    auto row = selected_columns != nullptr
                   ? StableReadValueMasked(*item, selected_columns->data(),
                                           selected_columns->size())
                   : StableReadValue(*item);
    if (row.found) {
      result.rows.push_back({std::string(secondary_key),
                             std::string(primary_key), std::move(row.value),
                             PackTransactionId(row.tid), true});
      ++returned_rows;
    }
    return row_limit > 0 && returned_rows >= row_limit;
  };

  auto append_secondary_entry = [&](std::string_view key) {
    const std::string secondary_key(key);
    DataItem *item = index->Get(key);
    if (item == nullptr) {
      return false;
    }

    auto slot = StableReadKeys(*item);
    for (std::string_view primary_key : slot.primary_keys_view()) {
      if (append_base_row(secondary_key, primary_key)) return true;
    }
    return false;
  };

  if (reverse_scan) {
    index->ScanReverse(start_key, end_key, append_secondary_entry);
  } else {
    index->Scan(start_key, end_key, append_secondary_entry);
  }
  return result;
}

ScanPaxResult ScanPax(TableDictionary &tables, std::shared_mutex &schema_mutex,
                      const std::string_view table_name,
                      const std::string_view start_key,
                      const std::string_view end_key, uint64_t row_limit,
                      bool reverse_scan) {
  ScanPaxResult result;
  if (end_key.empty()) return result;

  std::shared_lock<std::shared_mutex> lk(schema_mutex);
  auto table = tables.GetTable(table_name);
  if (!table.has_value()) return result;

  // PAX row references are only valid when every live row is in PAX strips.
  // Heap fallback rows are invisible to strip-only readers, so fall back.
  auto *store = table.value()->GetPaxStore();
  if (store == nullptr || store->overflow_count() > 0) return result;
  result.ok = true;

  uint64_t returned_rows = 0;
  bool saw_non_pax = false;

  auto append_ref = [&](std::string_view key, DataItem &item_ref) {
    // Observe the same stable unlocked TID that a materialized read would use.
    // The caller re-checks this TID after reading cells from the strip.
    TransactionId tid;
    for (;;) {
      tid = item_ref.transaction_id.load();
      if (!(tid.tid & 1u)) break;
      _mm_pause();
    }

    const size_t size = item_ref.buffer.size;
    if (size == 0) return false;
    // Mixed PAX/heap storage makes strip-only row references incomplete.
    if (!item_ref.buffer.is_pax() || !item_ref.buffer.pax_allocated()) {
      saw_non_pax = true;
      return true;
    }

    // Return a reference to the PAX location instead of gathering row bytes.
    result.rows.push_back({std::string(key), item_ref.buffer.pax_group(),
                           item_ref.buffer.pax_slot(),
                           static_cast<uint32_t>(size), PackTransactionId(tid),
                           &item_ref});
    ++returned_rows;
    return row_limit > 0 && returned_rows >= row_limit;
  };

  if (reverse_scan) {
    table.value()->GetPrimaryIndex().ScanReverse(start_key, end_key,
                                                 append_ref);
  } else {
    table.value()->GetPrimaryIndex().Scan(start_key, end_key, append_ref);
  }
  if (saw_non_pax) {
    // Keep the fallback contract simple: no partial refs escape on failure.
    result.ok = false;
    result.rows.clear();
  }
  return result;
}

}  // namespace silo
}  // namespace helios::storage
