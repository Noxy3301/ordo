#pragma once

// TPC-C-shaped workload for LDB direct-drive benchmark.
// No functional correctness — we only reproduce RPC shape + read/write key
// patterns + value sizes so OCC contention and MPMC/PL hotpaths see the same
// load as the MySQL path.

#include "client.hh"

#include <cstdint>
#include <string>

namespace bench_client {

struct TpccConfig {
    int warehouses = 1;
    int districts_per_warehouse = 10;
    int customers_per_district = 3000;
    int items = 100000;
};

// One-shot populate of all tables. Uses TxBatchWrite in batches of 100 rows.
// Caller provides an already-connected Client. Idempotent across restarts only
// in the sense that CREATE_TABLE is safe; duplicate row inserts will either be
// silently ignored or OCC-abort (we don't care here, populate assumes empty DB).
//
// Returns true on success.
bool populate(Client& client, const TpccConfig& cfg);

// Per-worker state for NewOrder execution. RNG stream, unique o_id range,
// scratch buffers reused across tx.
struct NewOrderWorker {
    int thread_id;
    const TpccConfig* cfg;
    uint64_t rng_state;
};

// Represents the outcome of one NewOrder attempt.
enum class TxOutcome { kCommitted, kAborted };

// Execute one NewOrder transaction (with retry logic handled by caller).
// Returns kAborted if the tx aborts at any stage; caller may retry.
// On wire error returns kAborted (caller treats same as OCC abort + retry).
TxOutcome run_new_order_once(Client& client, NewOrderWorker& w);

// Execute one Payment transaction (TPC-C standard: RMW warehouse/district/
// customer + INSERT history). Payment alone cannot produce aborts under LDB
// Silo (all fields are RMW on the same key, masked by the write-set TID
// overwrite). Mixed with NewOrder however, Payment writes warehouse and
// customer which NewOrder reads read-only — that is the only pattern that
// forces real OCC aborts.
TxOutcome run_payment_once(Client& client, NewOrderWorker& w);

// Execute one OrderStatus transaction. Read-only: customer + one recent oorder
// + that oorder's order_lines. Abort source: concurrent Payment writes to
// customer, or concurrent Delivery writes to order_line.
// Simplified vs BenchBase: skip by-name variant (no SI); pick o_id uniformly
// from [3001, 3001+1024) which NewOrder has been inserting into.
TxOutcome run_order_status_once(Client& client, NewOrderWorker& w);

// Execute one Delivery transaction. For each of 10 districts:
//   - Scan new_order (w, d, *) to find min o_id (range scan)
//   - DELETE new_order (w, d, o_id) (via blind write of zero-length value — LDB treats as tombstone)
//   - READ+WRITE oorder (w, d, o_id) (set o_carrier_id)
//   - For each order_line (w, d, o_id, ol): READ+WRITE (set ol_delivery_d)
//   - READ+WRITE customer (w, d, c_id) (bump c_balance / c_delivery_cnt)
TxOutcome run_delivery_once(Client& client, NewOrderWorker& w);

// Execute one StockLevel transaction. Read district → scan order_line range of
// last 20 orders → for each ol_i_id, read stock → count s_quantity < threshold.
// Read-only. Conflict source: concurrent NewOrder writes to stock.
TxOutcome run_stock_level_once(Client& client, NewOrderWorker& w);

// Names of tables we use. Kept as string_view-style constants for NewOrder later.
namespace tables {
constexpr const char* kWarehouse = "warehouse";
constexpr const char* kDistrict = "district";
constexpr const char* kCustomer = "customer";
constexpr const char* kItem = "item";
constexpr const char* kStock = "stock";
constexpr const char* kOorder = "oorder";
constexpr const char* kNewOrder = "new_order";
constexpr const char* kOrderLine = "order_line";
constexpr const char* kHistory = "history";
}  // namespace tables

// Value-size constants (~BenchBase sizes; only size matters for LDB load).
namespace value_sizes {
constexpr size_t kWarehouse = 96;     // w_name + w_street + w_city + tax/ytd
constexpr size_t kDistrict = 96;
constexpr size_t kCustomer = 672;
constexpr size_t kItem = 82;
constexpr size_t kStock = 306;
constexpr size_t kOorder = 32;
constexpr size_t kNewOrder = 8;
constexpr size_t kOrderLine = 64;
constexpr size_t kHistory = 46;
}  // namespace value_sizes

}  // namespace bench_client
