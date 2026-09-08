/**
 * @file server/storage/src/index/stats.cc
 * Index statistics a query layer turns into optimizer estimates: exact NDV
 * per key-part prefix and an equi-depth histogram of the leading key part.
 */

#include <xmmintrin.h>

#include <algorithm>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "database_impl.h"
#include "index/data_item.h"
#include "index/secondary_index.h"

namespace helios::storage {

namespace {

// Exclusive end of a whole-index scan: a stored key opens with the null
// marker of its first part, never with 0xFF.
const std::string kScanEnd(16, '\xff');

// Stable-read base-row liveness without copying the row payload.
bool StableLive(const DataItem &item) {
  for (;;) {
    TransactionId tid = item.transaction_id.load();
    if (tid.tid & 1u) {
      _mm_pause();
      continue;
    }

    const bool live = item.HasRow();
    if (item.transaction_id.load() == tid) return live;
  }
}

}  // namespace

bool Database::Impl::IndexNdv(const std::string_view table_name,
                              const std::string_view index_name,
                              uint32_t num_parts, const KeyParts &parts,
                              std::vector<uint64_t> &out_ndv) {
  out_ndv.assign(num_parts, 0);
  if (num_parts == 0) return false;

  std::shared_lock<std::shared_mutex> lk(schema_mutex_);
  auto table = GetTable(table_name);
  if (!table.has_value()) return false;

  bool ok = true;
  bool first = true;
  // Index scans are key-ordered, so one previous key is enough for NDV.
  std::string prev_key;
  std::vector<size_t> prev_part_ends(num_parts, 0);

  // NDV counts prefixes, so only a key the caller accepts counts.
  auto count_key = [&](std::string_view key) -> bool {
    std::vector<size_t> part_ends(num_parts, 0);
    if (!parts(key, num_parts, part_ends.data())) {
      ok = false;
      return true;
    }

    if (first) {
      // The first live key starts one distinct prefix at every depth.
      for (uint32_t part = 0; part < num_parts; ++part) out_ndv[part] = 1;
      first = false;
    } else {
      // Count a new prefix whenever bytes up to that key-part boundary
      // differ.
      const std::string_view prev(prev_key);
      for (uint32_t part = 0; part < num_parts; ++part) {
        if (part_ends[part] != prev_part_ends[part] ||
            key.substr(0, part_ends[part]) !=
                prev.substr(0, prev_part_ends[part])) {
          ++out_ndv[part];
        }
      }
    }

    prev_key.assign(key.data(), key.size());
    prev_part_ends = std::move(part_ends);
    return false;
  };

  auto &primary_index = table.value()->GetPrimaryIndex();

  if (index_name.empty()) {
    // Primary index entries are base rows, so count live rows directly.
    primary_index.Scan(std::string_view(), std::string_view(kScanEnd),
                       [&](std::string_view key, DataItem &item) -> bool {
                         if (!StableLive(item)) return false;
                         return count_key(key);
                       });
  } else {
    index::SecondaryIndex *index = table.value()->GetSecondaryIndex(index_name);
    if (index == nullptr) return false;

    // Pin the secondary primary-key list under one stable TID.
    auto stable_live_secondary = [&](const DataItem &item) {
      PackedPrimaryKeys::Ptr primary_keys;
      for (;;) {
        TransactionId tid = item.transaction_id.load();
        if (tid.tid & 1u) {
          _mm_pause();
          continue;
        }

        auto snapshot = std::atomic_load(&item.primary_keys_);
        const bool live = snapshot && snapshot->count != 0;
        if (item.transaction_id.load() == tid) {
          if (live) primary_keys = std::move(snapshot);
          break;
        }
      }

      // Secondary entries count only if one referenced base row is live.
      for (std::string_view primary_key : PackedPrimaryKeysView(primary_keys)) {
        DataItem *base_item = primary_index.Get(primary_key);
        if (base_item != nullptr && StableLive(*base_item)) return true;
      }
      return false;
    };

    index->Scan(std::string_view(), std::string_view(kScanEnd),
                [&](std::string_view key) -> bool {
                  DataItem *item = index->Get(key);
                  if (item == nullptr || !stable_live_secondary(*item)) {
                    return false;
                  }
                  return count_key(key);
                });
  }

  if (!ok) {
    // Fail closed: caller keeps the old optimizer estimate.
    out_ndv.assign(num_parts, 0);
    return false;
  }
  return true;
}

bool Database::Impl::IndexHistogram(const std::string_view table_name,
                                    const std::string_view index_name,
                                    uint32_t buckets, const KeyParts &parts,
                                    std::vector<std::string> &out_bounds,
                                    std::vector<uint64_t> &out_cum) {
  out_bounds.clear();
  out_cum.clear();
  if (buckets == 0) return false;
  std::shared_lock<std::shared_mutex> lk(schema_mutex_);
  auto table = GetTable(table_name);
  if (!table.has_value()) return false;

  // Histogram bounds are compared as bytes, so a key the caller refuses ends
  // the pass.
  auto leading_end = [&parts](std::string_view key) -> size_t {
    size_t end = 0;
    return parts(key, 1, &end) ? end : 0;
  };

  // Secondary scans visit one entry per key, but the histogram is over rows.
  // Use the PK-list length as that key's row weight.
  const auto stable_pk_count = [](DataItem &di) -> uint64_t {
    for (;;) {
      TransactionId tid = di.transaction_id.load();
      if (tid.tid & 1u) {
        _mm_pause();
        continue;
      }
      const auto primary_keys = std::atomic_load(&di.primary_keys_);
      const uint64_t n = primary_keys ? primary_keys->count : 0;
      if (di.transaction_id.load() == tid) return n;
    }
  };
  // Walk one index in key order and expose each live key with its row weight.
  bool malformed = false;
  auto walk = [&](auto &&fn) {
    if (index_name.empty()) {
      table.value()->GetPrimaryIndex().Scan(
          std::string_view(), std::string_view(kScanEnd),
          [&](std::string_view key, DataItem &di) -> bool {
            if (StableLive(di)) return fn(key, static_cast<uint64_t>(1));
            return false;
          });
    } else {
      index::SecondaryIndex *index =
          table.value()->GetSecondaryIndex(index_name);
      if (index == nullptr) {
        malformed = true;
        return;
      }
      index->Scan(std::string_view(), std::string_view(kScanEnd),
                  [&](std::string_view key) -> bool {
                    DataItem *item = index->Get(key);
                    if (item == nullptr) return false;
                    const uint64_t w = stable_pk_count(*item);
                    if (w == 0) return false;  // dead/empty secondary entry
                    return fn(key, w);
                  });
    }
  };

  // Pass 1: count total rows represented by the index.
  uint64_t total = 0;
  walk([&](std::string_view key, uint64_t w) -> bool {
    if (leading_end(key) == 0) {
      malformed = true;
      return true;
    }
    total += w;
    return false;
  });
  if (malformed || total == 0) return false;

  // Pass 2: record a boundary at each stride-th row.
  const uint64_t stride = std::max<uint64_t>(1, total / buckets);
  uint64_t seen = 0;
  uint64_t next = stride;
  std::string last_key;
  walk([&](std::string_view key, uint64_t w) -> bool {
    const size_t end = leading_end(key);
    if (end == 0) {
      malformed = true;
      return true;
    }
    seen += w;
    last_key.assign(key.data(), end);
    if (seen >= next) {
      out_bounds.emplace_back(key.substr(0, end));
      out_cum.push_back(seen);
      while (seen >= next) next += stride;
    }
    return false;
  });
  if (malformed) {
    out_bounds.clear();
    out_cum.clear();
    return false;
  }
  if (out_bounds.empty() || out_cum.back() != total) {
    // Close the histogram at the max key so the high end is exact.
    out_bounds.push_back(last_key);
    out_cum.push_back(total);
  }
  return !out_bounds.empty();
}

}  // namespace helios::storage
