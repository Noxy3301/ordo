#include "workload_tpcc.hh"

#include "key_encoder.hh"
#include "lineairdb.pb.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdint>
#include <string>
#include <vector>

namespace bench_client {

namespace {

constexpr int kBatchSize = 100;

std::string dummy_value(size_t n, char fill = 'x') {
    return std::string(n, fill);
}

// Layout helpers: we encode the "live" integer fields at fixed offsets inside
// the value blob so NewOrder can RMW them (d_next_o_id for district,
// s_quantity for stock). Little-endian int32 at caller-chosen offset.
inline void write_int32_le_at(std::string& s, size_t off, int32_t v) {
    s[off + 0] = static_cast<char>((v >> 0) & 0xFF);
    s[off + 1] = static_cast<char>((v >> 8) & 0xFF);
    s[off + 2] = static_cast<char>((v >> 16) & 0xFF);
    s[off + 3] = static_cast<char>((v >> 24) & 0xFF);
}
inline int32_t read_int32_le_at(const std::string& s, size_t off) {
    uint32_t v = static_cast<uint8_t>(s[off + 0]);
    v |= static_cast<uint32_t>(static_cast<uint8_t>(s[off + 1])) << 8;
    v |= static_cast<uint32_t>(static_cast<uint8_t>(s[off + 2])) << 16;
    v |= static_cast<uint32_t>(static_cast<uint8_t>(s[off + 3])) << 24;
    return static_cast<int32_t>(v);
}

bool create_table(Client& c, const std::string& name) {
    LineairDB::Protocol::DbCreateTable::Request req;
    req.set_table_name(name);
    LineairDB::Protocol::DbCreateTable::Response resp;
    if (!c.call(MessageType::DB_CREATE_TABLE, req, resp)) return false;
    // success may be false if table exists; that's fine.
    return true;
}

// Open a batch-write transaction, flush N WriteOps into it, commit.
// Returns true if committed (not aborted).
bool flush_batch(Client& c, const std::string& table,
                 const std::vector<std::pair<std::string, std::string>>& rows) {
    if (rows.empty()) return true;

    // BEGIN
    int64_t tx_id = 0;
    {
        LineairDB::Protocol::TxBeginTransaction::Request req;
        LineairDB::Protocol::TxBeginTransaction::Response resp;
        if (!c.call(MessageType::TX_BEGIN_TRANSACTION, req, resp)) return false;
        tx_id = resp.transaction_id();
    }

    // SET_TABLE
    {
        LineairDB::Protocol::DbSetTable::Request req;
        req.set_transaction_id(tx_id);
        req.set_table_name(table);
        LineairDB::Protocol::DbSetTable::Response resp;
        if (!c.call(MessageType::DB_SET_TABLE, req, resp)) return false;
        if (!resp.success()) return false;
    }

    // BATCH_WRITE
    {
        LineairDB::Protocol::TxBatchWrite::Request req;
        req.set_transaction_id(tx_id);
        req.set_table_name(table);
        for (const auto& kv : rows) {
            auto* op = req.add_writes();
            op->set_key(kv.first);
            op->set_value(kv.second);
        }
        LineairDB::Protocol::TxBatchWrite::Response resp;
        if (!c.call(MessageType::TX_BATCH_WRITE, req, resp)) return false;
        if (resp.is_aborted() || !resp.success()) return false;
    }

    // END
    {
        LineairDB::Protocol::DbEndTransaction::Request req;
        req.set_transaction_id(tx_id);
        req.set_fence(false);
        LineairDB::Protocol::DbEndTransaction::Response resp;
        if (!c.call(MessageType::DB_END_TRANSACTION, req, resp)) return false;
        if (resp.is_aborted()) return false;
    }
    return true;
}

bool populate_warehouse(Client& c, const TpccConfig& cfg) {
    std::vector<std::pair<std::string, std::string>> rows;
    rows.reserve(cfg.warehouses);
    // Initial w_ytd = 300000 at offset 0 (TPC-C spec, cents). Payment RMW target.
    std::string v = dummy_value(value_sizes::kWarehouse, 'w');
    write_int32_le_at(v, 0, 300000);
    for (int w = 1; w <= cfg.warehouses; ++w) {
        rows.emplace_back(make_int_key(w), v);
    }
    return flush_batch(c, tables::kWarehouse, rows);
}

bool populate_district(Client& c, const TpccConfig& cfg) {
    std::vector<std::pair<std::string, std::string>> rows;
    // District layout:
    //   offset 0: d_next_o_id (int32, NewOrder RMW target), init 3001
    //   offset 4: d_ytd       (int32, Payment RMW target),  init 30000
    std::string v = dummy_value(value_sizes::kDistrict, 'd');
    write_int32_le_at(v, 0, 3001);
    write_int32_le_at(v, 4, 30000);
    for (int w = 1; w <= cfg.warehouses; ++w) {
        for (int d = 1; d <= cfg.districts_per_warehouse; ++d) {
            rows.emplace_back(make_int_key(w, d), v);
        }
    }
    return flush_batch(c, tables::kDistrict, rows);
}

bool populate_customer(Client& c, const TpccConfig& cfg) {
    std::vector<std::pair<std::string, std::string>> rows;
    rows.reserve(kBatchSize);
    // Initial c_balance = -10 at offset 0 (TPC-C spec cents). Payment RMW target.
    std::string v = dummy_value(value_sizes::kCustomer, 'c');
    write_int32_le_at(v, 0, -10);
    for (int w = 1; w <= cfg.warehouses; ++w) {
        for (int d = 1; d <= cfg.districts_per_warehouse; ++d) {
            for (int ci = 1; ci <= cfg.customers_per_district; ++ci) {
                rows.emplace_back(make_int_key(w, d, ci), v);
                if (static_cast<int>(rows.size()) >= kBatchSize) {
                    if (!flush_batch(c, tables::kCustomer, rows)) return false;
                    rows.clear();
                }
            }
        }
    }
    return flush_batch(c, tables::kCustomer, rows);
}

bool populate_item(Client& c, const TpccConfig& cfg) {
    std::vector<std::pair<std::string, std::string>> rows;
    rows.reserve(kBatchSize);
    const std::string v = dummy_value(value_sizes::kItem, 'i');
    for (int i = 1; i <= cfg.items; ++i) {
        rows.emplace_back(make_int_key(i), v);
        if (static_cast<int>(rows.size()) >= kBatchSize) {
            if (!flush_batch(c, tables::kItem, rows)) return false;
            rows.clear();
        }
    }
    return flush_batch(c, tables::kItem, rows);
}

bool populate_stock(Client& c, const TpccConfig& cfg) {
    std::vector<std::pair<std::string, std::string>> rows;
    rows.reserve(kBatchSize);
    // Initial s_quantity = 100 at offset 0 (TPC-C spec).
    std::string v = dummy_value(value_sizes::kStock, 's');
    write_int32_le_at(v, 0, 100);
    for (int w = 1; w <= cfg.warehouses; ++w) {
        for (int i = 1; i <= cfg.items; ++i) {
            rows.emplace_back(make_int_key(w, i), v);
            if (static_cast<int>(rows.size()) >= kBatchSize) {
                if (!flush_batch(c, tables::kStock, rows)) return false;
                rows.clear();
            }
        }
    }
    return flush_batch(c, tables::kStock, rows);
}

}  // namespace

bool populate(Client& client, const TpccConfig& cfg) {
    const auto t0 = std::chrono::steady_clock::now();

    // Create tables. Dynamic tables (oorder/new_order/order_line) filled at
    // runtime by NewOrder; still create them up front so SET_TABLE works.
    for (const char* name : {tables::kWarehouse, tables::kDistrict, tables::kCustomer,
                             tables::kItem, tables::kStock, tables::kOorder,
                             tables::kNewOrder, tables::kOrderLine, tables::kHistory}) {
        if (!create_table(client, name)) {
            std::fprintf(stderr, "populate: CREATE_TABLE %s failed\n", name);
            return false;
        }
    }
    std::printf("populate: tables created\n");

    if (!populate_warehouse(client, cfg)) { std::fprintf(stderr, "populate: warehouse failed\n"); return false; }
    std::printf("populate: warehouse %d rows\n", cfg.warehouses);

    if (!populate_district(client, cfg)) { std::fprintf(stderr, "populate: district failed\n"); return false; }
    std::printf("populate: district %d rows\n", cfg.warehouses * cfg.districts_per_warehouse);

    if (!populate_customer(client, cfg)) { std::fprintf(stderr, "populate: customer failed\n"); return false; }
    const int cust_total = cfg.warehouses * cfg.districts_per_warehouse * cfg.customers_per_district;
    std::printf("populate: customer %d rows\n", cust_total);

    if (!populate_item(client, cfg)) { std::fprintf(stderr, "populate: item failed\n"); return false; }
    std::printf("populate: item %d rows\n", cfg.items);

    if (!populate_stock(client, cfg)) { std::fprintf(stderr, "populate: stock failed\n"); return false; }
    std::printf("populate: stock %d rows\n", cfg.warehouses * cfg.items);

    const double elapsed =
        std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
    std::printf("populate: done in %.2fs\n", elapsed);
    return true;
}

// -----------------------------------------------------------------------------
// NewOrder execution
// -----------------------------------------------------------------------------

namespace {

// xorshift64* — cheap, good enough for random item/customer picking.
inline uint64_t xorshift64(uint64_t& s) {
    s ^= s >> 12;
    s ^= s << 25;
    s ^= s >> 27;
    return s * 2685821657736338717ULL;
}
inline int rand_range(uint64_t& s, int lo, int hi) {
    return lo + static_cast<int>(xorshift64(s) % static_cast<uint64_t>(hi - lo + 1));
}

// Thread-local scratch values, sized to match the schema. Same bytes every tx
// except for district/stock/warehouse/customer which carry RMW-mutated fields
// (those tables go through explicit decode+re-encode in the tx body).
const std::string& value_of(const char* table) {
    static const std::string v_it = std::string(value_sizes::kItem, 'i');
    static const std::string v_oo = std::string(value_sizes::kOorder, 'o');
    static const std::string v_no = std::string(value_sizes::kNewOrder, 'n');
    static const std::string v_ol = std::string(value_sizes::kOrderLine, 'l');
    static const std::string v_hi = std::string(value_sizes::kHistory, 'h');
    if (table == tables::kItem) return v_it;
    if (table == tables::kOorder) return v_oo;
    if (table == tables::kNewOrder) return v_no;
    if (table == tables::kHistory) return v_hi;
    return v_ol;
}

}  // namespace

TxOutcome run_new_order_once(Client& client, NewOrderWorker& w) {
    // Pick inputs (BenchBase NewOrder.java 相当)
    const int w_id = 1 + rand_range(w.rng_state, 0, w.cfg->warehouses - 1);
    const int d_id = 1 + rand_range(w.rng_state, 0, w.cfg->districts_per_warehouse - 1);
    const int c_id = 1 + rand_range(w.rng_state, 0, w.cfg->customers_per_district - 1);
    const int ol_cnt = rand_range(w.rng_state, 5, 15);

    // BEGIN
    int64_t tx_id = 0;
    {
        LineairDB::Protocol::TxBeginTransaction::Request req;
        LineairDB::Protocol::TxBeginTransaction::Response resp;
        if (!client.call(MessageType::TX_BEGIN_TRANSACTION, req, resp)) return TxOutcome::kAborted;
        tx_id = resp.transaction_id();
    }

    auto finish_abort = [&]() -> TxOutcome {
        // Some aborts come mid-stream; still send END to keep server state clean.
        LineairDB::Protocol::DbEndTransaction::Request req;
        req.set_transaction_id(tx_id);
        req.set_fence(false);
        LineairDB::Protocol::DbEndTransaction::Response resp;
        (void)client.call(MessageType::DB_END_TRANSACTION, req, resp);
        return TxOutcome::kAborted;
    };

    // Note: DB_SET_TABLE is intentionally omitted. Every TxRead/TxWrite/TxBatchWrite
    // request carries `table_name`; the server handler calls SetTable() each time.
    // An extra standalone SET_TABLE RPC would just inflate latency.

    // customer READ
    {
        LineairDB::Protocol::TxRead::Request req;
        req.set_transaction_id(tx_id);
        req.set_table_name(tables::kCustomer);
        req.set_key(make_int_key(w_id, d_id, c_id));
        LineairDB::Protocol::TxRead::Response resp;
        if (!client.call(MessageType::TX_READ, req, resp)) return finish_abort();
        if (resp.is_aborted()) return finish_abort();
    }

    // warehouse READ
    {
        LineairDB::Protocol::TxRead::Request req;
        req.set_transaction_id(tx_id);
        req.set_table_name(tables::kWarehouse);
        req.set_key(make_int_key(w_id));
        LineairDB::Protocol::TxRead::Response resp;
        if (!client.call(MessageType::TX_READ, req, resp)) return finish_abort();
        if (resp.is_aborted()) return finish_abort();
    }

    // district READ → consume d_next_o_id → WRITE with d_next_o_id+1
    const std::string district_key = make_int_key(w_id, d_id);
    int32_t o_id = 0;
    std::string district_value;
    {
        LineairDB::Protocol::TxRead::Request req;
        req.set_transaction_id(tx_id);
        req.set_table_name(tables::kDistrict);
        req.set_key(district_key);
        LineairDB::Protocol::TxRead::Response resp;
        if (!client.call(MessageType::TX_READ, req, resp)) return finish_abort();
        if (resp.is_aborted()) return finish_abort();
        if (!resp.found() || resp.value().size() < 4) return finish_abort();
        district_value = resp.value();
        o_id = read_int32_le_at(district_value, 0);
    }
    {
        LineairDB::Protocol::TxWrite::Request req;
        req.set_transaction_id(tx_id);
        req.set_table_name(tables::kDistrict);
        req.set_key(district_key);
        // Real RMW: bump d_next_o_id so concurrent tx observe version change.
        write_int32_le_at(district_value, 0, o_id + 1);
        req.set_value(district_value);
        LineairDB::Protocol::TxWrite::Response resp;
        if (!client.call(MessageType::TX_WRITE, req, resp)) return finish_abort();
        if (resp.is_aborted() || !resp.success()) return finish_abort();
    }

    // BATCH_WRITE [oorder (w,d,o_id)] then [new_order (w,d,o_id)]
    {
        LineairDB::Protocol::TxBatchWrite::Request req;
        req.set_transaction_id(tx_id);
        req.set_table_name(tables::kOorder);
        auto* op = req.add_writes();
        op->set_key(make_int_key(w_id, d_id, o_id));
        op->set_value(value_of(tables::kOorder));
        LineairDB::Protocol::TxBatchWrite::Response resp;
        if (!client.call(MessageType::TX_BATCH_WRITE, req, resp)) return finish_abort();
        if (resp.is_aborted() || !resp.success()) return finish_abort();
    }
    {
        LineairDB::Protocol::TxBatchWrite::Request req;
        req.set_transaction_id(tx_id);
        req.set_table_name(tables::kNewOrder);
        auto* op = req.add_writes();
        op->set_key(make_int_key(w_id, d_id, o_id));
        op->set_value(value_of(tables::kNewOrder));
        LineairDB::Protocol::TxBatchWrite::Response resp;
        if (!client.call(MessageType::TX_BATCH_WRITE, req, resp)) return finish_abort();
        if (resp.is_aborted() || !resp.success()) return finish_abort();
    }

    // Per order-line: READ item, READ+WRITE stock (real RMW on s_quantity)
    for (int ol = 1; ol <= ol_cnt; ++ol) {
        const int i_id = 1 + rand_range(w.rng_state, 0, w.cfg->items - 1);
        {
            LineairDB::Protocol::TxRead::Request req;
            req.set_transaction_id(tx_id);
            req.set_table_name(tables::kItem);
            req.set_key(make_int_key(i_id));
            LineairDB::Protocol::TxRead::Response resp;
            if (!client.call(MessageType::TX_READ, req, resp)) return finish_abort();
            if (resp.is_aborted()) return finish_abort();
        }
        const std::string stock_key = make_int_key(w_id, i_id);
        std::string stock_value;
        {
            LineairDB::Protocol::TxRead::Request req;
            req.set_transaction_id(tx_id);
            req.set_table_name(tables::kStock);
            req.set_key(stock_key);
            LineairDB::Protocol::TxRead::Response resp;
            if (!client.call(MessageType::TX_READ, req, resp)) return finish_abort();
            if (resp.is_aborted()) return finish_abort();
            if (!resp.found() || resp.value().size() < 4) return finish_abort();
            stock_value = resp.value();
        }
        {
            LineairDB::Protocol::TxWrite::Request req;
            req.set_transaction_id(tx_id);
            req.set_table_name(tables::kStock);
            req.set_key(stock_key);
            // Real RMW: decrement s_quantity (with floor + replenish, TPC-C spec).
            int32_t q = read_int32_le_at(stock_value, 0);
            const int order_qty = 1 + rand_range(w.rng_state, 0, 9);  // 1..10
            q = (q >= order_qty + 10) ? (q - order_qty) : (q - order_qty + 91);
            write_int32_le_at(stock_value, 0, q);
            req.set_value(stock_value);
            LineairDB::Protocol::TxWrite::Response resp;
            if (!client.call(MessageType::TX_WRITE, req, resp)) return finish_abort();
            if (resp.is_aborted() || !resp.success()) return finish_abort();
        }
    }

    // BATCH_WRITE [order_line * ol_cnt]
    {
        LineairDB::Protocol::TxBatchWrite::Request req;
        req.set_transaction_id(tx_id);
        req.set_table_name(tables::kOrderLine);
        for (int ol = 1; ol <= ol_cnt; ++ol) {
            auto* op = req.add_writes();
            op->set_key(make_int_key(w_id, d_id, o_id, ol));
            op->set_value(value_of(tables::kOrderLine));
        }
        LineairDB::Protocol::TxBatchWrite::Response resp;
        if (!client.call(MessageType::TX_BATCH_WRITE, req, resp)) return finish_abort();
        if (resp.is_aborted() || !resp.success()) return finish_abort();
    }

    // END
    {
        LineairDB::Protocol::DbEndTransaction::Request req;
        req.set_transaction_id(tx_id);
        req.set_fence(false);
        LineairDB::Protocol::DbEndTransaction::Response resp;
        if (!client.call(MessageType::DB_END_TRANSACTION, req, resp)) return TxOutcome::kAborted;
        if (resp.is_aborted()) return TxOutcome::kAborted;
    }

    return TxOutcome::kCommitted;
}

// -----------------------------------------------------------------------------
// Payment execution
// -----------------------------------------------------------------------------

namespace {
// Per-worker rolling counter for history INSERT key uniqueness. Thread-local
// (one counter per thread) via function-static + thread_local.
thread_local uint64_t s_history_seq = 0;
}

TxOutcome run_payment_once(Client& client, NewOrderWorker& w) {
    const int w_id = 1 + rand_range(w.rng_state, 0, w.cfg->warehouses - 1);
    const int d_id = 1 + rand_range(w.rng_state, 0, w.cfg->districts_per_warehouse - 1);
    const int c_id = 1 + rand_range(w.rng_state, 0, w.cfg->customers_per_district - 1);
    const int32_t h_amount = 1 + static_cast<int32_t>(xorshift64(w.rng_state) % 5000);

    // BEGIN
    int64_t tx_id = 0;
    {
        LineairDB::Protocol::TxBeginTransaction::Request req;
        LineairDB::Protocol::TxBeginTransaction::Response resp;
        if (!client.call(MessageType::TX_BEGIN_TRANSACTION, req, resp)) return TxOutcome::kAborted;
        tx_id = resp.transaction_id();
    }

    auto finish_abort = [&]() -> TxOutcome {
        LineairDB::Protocol::DbEndTransaction::Request req;
        req.set_transaction_id(tx_id);
        req.set_fence(false);
        LineairDB::Protocol::DbEndTransaction::Response resp;
        (void)client.call(MessageType::DB_END_TRANSACTION, req, resp);
        return TxOutcome::kAborted;
    };

    // warehouse RMW (w_ytd += h_amount)
    const std::string wh_key = make_int_key(w_id);
    std::string wh_value;
    {
        LineairDB::Protocol::TxRead::Request req;
        req.set_transaction_id(tx_id);
        req.set_table_name(tables::kWarehouse);
        req.set_key(wh_key);
        LineairDB::Protocol::TxRead::Response resp;
        if (!client.call(MessageType::TX_READ, req, resp)) return finish_abort();
        if (resp.is_aborted()) return finish_abort();
        if (!resp.found() || resp.value().size() < 4) return finish_abort();
        wh_value = resp.value();
    }
    {
        LineairDB::Protocol::TxWrite::Request req;
        req.set_transaction_id(tx_id);
        req.set_table_name(tables::kWarehouse);
        req.set_key(wh_key);
        int32_t w_ytd = read_int32_le_at(wh_value, 0);
        write_int32_le_at(wh_value, 0, w_ytd + h_amount);
        req.set_value(wh_value);
        LineairDB::Protocol::TxWrite::Response resp;
        if (!client.call(MessageType::TX_WRITE, req, resp)) return finish_abort();
        if (resp.is_aborted() || !resp.success()) return finish_abort();
    }

    // district RMW (d_ytd += h_amount, offset 4 — leaves d_next_o_id at offset 0)
    const std::string di_key = make_int_key(w_id, d_id);
    std::string di_value;
    {
        LineairDB::Protocol::TxRead::Request req;
        req.set_transaction_id(tx_id);
        req.set_table_name(tables::kDistrict);
        req.set_key(di_key);
        LineairDB::Protocol::TxRead::Response resp;
        if (!client.call(MessageType::TX_READ, req, resp)) return finish_abort();
        if (resp.is_aborted()) return finish_abort();
        if (!resp.found() || resp.value().size() < 8) return finish_abort();
        di_value = resp.value();
    }
    {
        LineairDB::Protocol::TxWrite::Request req;
        req.set_transaction_id(tx_id);
        req.set_table_name(tables::kDistrict);
        req.set_key(di_key);
        int32_t d_ytd = read_int32_le_at(di_value, 4);
        write_int32_le_at(di_value, 4, d_ytd + h_amount);
        req.set_value(di_value);
        LineairDB::Protocol::TxWrite::Response resp;
        if (!client.call(MessageType::TX_WRITE, req, resp)) return finish_abort();
        if (resp.is_aborted() || !resp.success()) return finish_abort();
    }

    // customer RMW (c_balance -= h_amount)
    const std::string cu_key = make_int_key(w_id, d_id, c_id);
    std::string cu_value;
    {
        LineairDB::Protocol::TxRead::Request req;
        req.set_transaction_id(tx_id);
        req.set_table_name(tables::kCustomer);
        req.set_key(cu_key);
        LineairDB::Protocol::TxRead::Response resp;
        if (!client.call(MessageType::TX_READ, req, resp)) return finish_abort();
        if (resp.is_aborted()) return finish_abort();
        if (!resp.found() || resp.value().size() < 4) return finish_abort();
        cu_value = resp.value();
    }
    {
        LineairDB::Protocol::TxWrite::Request req;
        req.set_transaction_id(tx_id);
        req.set_table_name(tables::kCustomer);
        req.set_key(cu_key);
        int32_t c_balance = read_int32_le_at(cu_value, 0);
        write_int32_le_at(cu_value, 0, c_balance - h_amount);
        req.set_value(cu_value);
        LineairDB::Protocol::TxWrite::Response resp;
        if (!client.call(MessageType::TX_WRITE, req, resp)) return finish_abort();
        if (resp.is_aborted() || !resp.success()) return finish_abort();
    }

    // history INSERT — thread-unique key so no cross-tx collision.
    {
        LineairDB::Protocol::TxBatchWrite::Request req;
        req.set_transaction_id(tx_id);
        req.set_table_name(tables::kHistory);
        auto* op = req.add_writes();
        // key = (thread_id, seq). Both int32, no collision across threads.
        op->set_key(make_int_key(w.thread_id, static_cast<int32_t>(s_history_seq++)));
        op->set_value(value_of(tables::kHistory));
        LineairDB::Protocol::TxBatchWrite::Response resp;
        if (!client.call(MessageType::TX_BATCH_WRITE, req, resp)) return finish_abort();
        if (resp.is_aborted() || !resp.success()) return finish_abort();
    }

    // END
    {
        LineairDB::Protocol::DbEndTransaction::Request req;
        req.set_transaction_id(tx_id);
        req.set_fence(false);
        LineairDB::Protocol::DbEndTransaction::Response resp;
        if (!client.call(MessageType::DB_END_TRANSACTION, req, resp)) return TxOutcome::kAborted;
        if (resp.is_aborted()) return TxOutcome::kAborted;
    }
    return TxOutcome::kCommitted;
}

// -----------------------------------------------------------------------------
// OrderStatus execution (simplified: PK-only, skip by-name variant)
// -----------------------------------------------------------------------------

TxOutcome run_order_status_once(Client& client, NewOrderWorker& w) {
    const int w_id = 1 + rand_range(w.rng_state, 0, w.cfg->warehouses - 1);
    const int d_id = 1 + rand_range(w.rng_state, 0, w.cfg->districts_per_warehouse - 1);
    const int c_id = 1 + rand_range(w.rng_state, 0, w.cfg->customers_per_district - 1);

    int64_t tx_id = 0;
    {
        LineairDB::Protocol::TxBeginTransaction::Request req;
        LineairDB::Protocol::TxBeginTransaction::Response resp;
        if (!client.call(MessageType::TX_BEGIN_TRANSACTION, req, resp)) return TxOutcome::kAborted;
        tx_id = resp.transaction_id();
    }
    auto finish_abort = [&]() -> TxOutcome {
        LineairDB::Protocol::DbEndTransaction::Request req;
        req.set_transaction_id(tx_id);
        req.set_fence(false);
        LineairDB::Protocol::DbEndTransaction::Response resp;
        (void)client.call(MessageType::DB_END_TRANSACTION, req, resp);
        return TxOutcome::kAborted;
    };

    // district READ → get live d_next_o_id so we hit the latest-order prefix,
    // not a stale fixed [3001,4024) window.
    int32_t d_next_o_id = 3001;
    {
        LineairDB::Protocol::TxRead::Request req;
        req.set_transaction_id(tx_id);
        req.set_table_name(tables::kDistrict);
        req.set_key(make_int_key(w_id, d_id));
        LineairDB::Protocol::TxRead::Response resp;
        if (!client.call(MessageType::TX_READ, req, resp)) return finish_abort();
        if (resp.is_aborted()) return finish_abort();
        if (resp.found() && resp.value().size() >= 4) {
            d_next_o_id = read_int32_le_at(resp.value(), 0);
        }
    }
    // Pick o_id uniformly over the last up-to-1024 committed orders. The
    // window is `[max(3001, d_next_o_id - 1024), d_next_o_id - 1]`, so during
    // warmup it may be narrower than 1024 entries. TPC-C §2.6.1.2 only
    // requires picking *a* recent order; the 1024 cap bounds workload spread.
    int32_t o_id = 3001;
    {
        const int32_t top = (d_next_o_id > 3001) ? (d_next_o_id - 1) : 3001;
        const int32_t bottom = (top > 3001 + 1023) ? (top - 1023) : 3001;
        const uint32_t span = static_cast<uint32_t>(top - bottom + 1);
        o_id = bottom + static_cast<int32_t>(xorshift64(w.rng_state) % span);
    }

    // customer READ
    {
        LineairDB::Protocol::TxRead::Request req;
        req.set_transaction_id(tx_id);
        req.set_table_name(tables::kCustomer);
        req.set_key(make_int_key(w_id, d_id, c_id));
        LineairDB::Protocol::TxRead::Response resp;
        if (!client.call(MessageType::TX_READ, req, resp)) return finish_abort();
        if (resp.is_aborted()) return finish_abort();
    }
    // oorder READ (may not exist if o_id wasn't produced yet — ignore not-found)
    {
        LineairDB::Protocol::TxRead::Request req;
        req.set_transaction_id(tx_id);
        req.set_table_name(tables::kOorder);
        req.set_key(make_int_key(w_id, d_id, o_id));
        LineairDB::Protocol::TxRead::Response resp;
        if (!client.call(MessageType::TX_READ, req, resp)) return finish_abort();
        if (resp.is_aborted()) return finish_abort();
    }
    // order_line READ ×10 (ol_number 1..10)
    for (int ol = 1; ol <= 10; ++ol) {
        LineairDB::Protocol::TxRead::Request req;
        req.set_transaction_id(tx_id);
        req.set_table_name(tables::kOrderLine);
        req.set_key(make_int_key(w_id, d_id, o_id, ol));
        LineairDB::Protocol::TxRead::Response resp;
        if (!client.call(MessageType::TX_READ, req, resp)) return finish_abort();
        if (resp.is_aborted()) return finish_abort();
    }

    {
        LineairDB::Protocol::DbEndTransaction::Request req;
        req.set_transaction_id(tx_id);
        req.set_fence(false);
        LineairDB::Protocol::DbEndTransaction::Response resp;
        if (!client.call(MessageType::DB_END_TRANSACTION, req, resp)) return TxOutcome::kAborted;
        if (resp.is_aborted()) return TxOutcome::kAborted;
    }
    return TxOutcome::kCommitted;
}

// -----------------------------------------------------------------------------
// Delivery execution (simplified: skip scan-for-min-o_id, use deterministic
// per-(w,d) counter for which o_id to "deliver")
// -----------------------------------------------------------------------------

namespace {
// Process-wide per-(w, d) delivery cursor, incremented atomically so threads
// never redeliver the same o_id. Sized for up to 64 warehouses × 64 districts;
// larger configurations would need a map, but bench_client targets SF=1.
constexpr int kMaxWh = 64;
constexpr int kMaxDi = 64;
std::atomic<int32_t> g_delivery_cursor[kMaxWh][kMaxDi];

// Lazy init: first read returns 3001 (TPC-C initial d_next_o_id) without
// double-initializing across threads.
int32_t next_delivery_oid(int w_id, int d_id) {
    auto& slot = g_delivery_cursor[w_id - 1][d_id - 1];
    int32_t cur = slot.load(std::memory_order_relaxed);
    while (cur == 0) {
        int32_t expected = 0;
        if (slot.compare_exchange_weak(expected, 3001, std::memory_order_relaxed)) {
            cur = 3001;
            break;
        }
        cur = slot.load(std::memory_order_relaxed);
    }
    return slot.fetch_add(1, std::memory_order_relaxed);
}
}

TxOutcome run_delivery_once(Client& client, NewOrderWorker& w) {
    const int w_id = 1 + rand_range(w.rng_state, 0, w.cfg->warehouses - 1);
    const int carrier_id = 1 + rand_range(w.rng_state, 0, 9);
    (void)carrier_id;  // value is written into oorder bytes; we don't decode it

    int64_t tx_id = 0;
    {
        LineairDB::Protocol::TxBeginTransaction::Request req;
        LineairDB::Protocol::TxBeginTransaction::Response resp;
        if (!client.call(MessageType::TX_BEGIN_TRANSACTION, req, resp)) return TxOutcome::kAborted;
        tx_id = resp.transaction_id();
    }
    auto finish_abort = [&]() -> TxOutcome {
        LineairDB::Protocol::DbEndTransaction::Request req;
        req.set_transaction_id(tx_id);
        req.set_fence(false);
        LineairDB::Protocol::DbEndTransaction::Response resp;
        (void)client.call(MessageType::DB_END_TRANSACTION, req, resp);
        return TxOutcome::kAborted;
    };

    for (int d_id = 1;
         d_id <= w.cfg->districts_per_warehouse && d_id <= kMaxDi && w_id <= kMaxWh;
         ++d_id) {
        const int32_t o_id = next_delivery_oid(w_id, d_id);

        // Check oorder exists first. If the NewOrder that produced this o_id
        // hasn't committed yet (or we're ahead of NewOrder), skip this district
        // without mutating new_order — avoids emitting tombstones for rows that
        // don't yet have a real insertion.
        {
            LineairDB::Protocol::TxRead::Request req;
            req.set_transaction_id(tx_id);
            req.set_table_name(tables::kOorder);
            req.set_key(make_int_key(w_id, d_id, o_id));
            LineairDB::Protocol::TxRead::Response resp;
            if (!client.call(MessageType::TX_READ, req, resp)) return finish_abort();
            if (resp.is_aborted()) return finish_abort();
            if (!resp.found()) continue;
        }

        // new_order DELETE (queue-drain semantics). Uses the dedicated TX_DELETE
        // RPC, which LDB routes through Transaction::Impl::Delete — this path
        // bumps the item's TID via an actual tombstone operation, so any
        // concurrent reader's validation_set_ entry becomes stale.
        {
            LineairDB::Protocol::TxDelete::Request req;
            req.set_transaction_id(tx_id);
            req.set_table_name(tables::kNewOrder);
            req.set_key(make_int_key(w_id, d_id, o_id));
            LineairDB::Protocol::TxDelete::Response resp;
            if (!client.call(MessageType::TX_DELETE, req, resp)) return finish_abort();
            if (resp.is_aborted() || !resp.success()) return finish_abort();
        }
        {
            LineairDB::Protocol::TxWrite::Request req;
            req.set_transaction_id(tx_id);
            req.set_table_name(tables::kOorder);
            req.set_key(make_int_key(w_id, d_id, o_id));
            req.set_value(value_of(tables::kOorder));
            LineairDB::Protocol::TxWrite::Response resp;
            if (!client.call(MessageType::TX_WRITE, req, resp)) return finish_abort();
            if (resp.is_aborted() || !resp.success()) return finish_abort();
        }
        // order_line RMW ×10 (set ol_delivery_d)
        for (int ol = 1; ol <= 10; ++ol) {
            {
                LineairDB::Protocol::TxRead::Request req;
                req.set_transaction_id(tx_id);
                req.set_table_name(tables::kOrderLine);
                req.set_key(make_int_key(w_id, d_id, o_id, ol));
                LineairDB::Protocol::TxRead::Response resp;
                if (!client.call(MessageType::TX_READ, req, resp)) return finish_abort();
                if (resp.is_aborted()) return finish_abort();
                if (!resp.found()) continue;
            }
            {
                LineairDB::Protocol::TxWrite::Request req;
                req.set_transaction_id(tx_id);
                req.set_table_name(tables::kOrderLine);
                req.set_key(make_int_key(w_id, d_id, o_id, ol));
                req.set_value(value_of(tables::kOrderLine));
                LineairDB::Protocol::TxWrite::Response resp;
                if (!client.call(MessageType::TX_WRITE, req, resp)) return finish_abort();
                if (resp.is_aborted() || !resp.success()) return finish_abort();
            }
        }
        // customer RMW (c_balance += sum; pick random c_id since we didn't look it up)
        const int c_id = 1 + rand_range(w.rng_state, 0, w.cfg->customers_per_district - 1);
        const std::string cu_key = make_int_key(w_id, d_id, c_id);
        std::string cu_value;
        {
            LineairDB::Protocol::TxRead::Request req;
            req.set_transaction_id(tx_id);
            req.set_table_name(tables::kCustomer);
            req.set_key(cu_key);
            LineairDB::Protocol::TxRead::Response resp;
            if (!client.call(MessageType::TX_READ, req, resp)) return finish_abort();
            if (resp.is_aborted()) return finish_abort();
            if (!resp.found() || resp.value().size() < 4) return finish_abort();
            cu_value = resp.value();
        }
        {
            LineairDB::Protocol::TxWrite::Request req;
            req.set_transaction_id(tx_id);
            req.set_table_name(tables::kCustomer);
            req.set_key(cu_key);
            int32_t balance = read_int32_le_at(cu_value, 0);
            write_int32_le_at(cu_value, 0, balance + 100);
            req.set_value(cu_value);
            LineairDB::Protocol::TxWrite::Response resp;
            if (!client.call(MessageType::TX_WRITE, req, resp)) return finish_abort();
            if (resp.is_aborted() || !resp.success()) return finish_abort();
        }
    }

    {
        LineairDB::Protocol::DbEndTransaction::Request req;
        req.set_transaction_id(tx_id);
        req.set_fence(false);
        LineairDB::Protocol::DbEndTransaction::Response resp;
        if (!client.call(MessageType::DB_END_TRANSACTION, req, resp)) return TxOutcome::kAborted;
        if (resp.is_aborted()) return TxOutcome::kAborted;
    }
    return TxOutcome::kCommitted;
}

// -----------------------------------------------------------------------------
// StockLevel execution (read-only; uses point reads, skips the real scan to
// avoid needing range-scan RPC infrastructure)
// -----------------------------------------------------------------------------

TxOutcome run_stock_level_once(Client& client, NewOrderWorker& w) {
    const int w_id = 1 + rand_range(w.rng_state, 0, w.cfg->warehouses - 1);
    const int d_id = 1 + rand_range(w.rng_state, 0, w.cfg->districts_per_warehouse - 1);

    int64_t tx_id = 0;
    {
        LineairDB::Protocol::TxBeginTransaction::Request req;
        LineairDB::Protocol::TxBeginTransaction::Response resp;
        if (!client.call(MessageType::TX_BEGIN_TRANSACTION, req, resp)) return TxOutcome::kAborted;
        tx_id = resp.transaction_id();
    }
    auto finish_abort = [&]() -> TxOutcome {
        LineairDB::Protocol::DbEndTransaction::Request req;
        req.set_transaction_id(tx_id);
        req.set_fence(false);
        LineairDB::Protocol::DbEndTransaction::Response resp;
        (void)client.call(MessageType::DB_END_TRANSACTION, req, resp);
        return TxOutcome::kAborted;
    };

    // District READ to find d_next_o_id
    int32_t d_next_o_id = 3001;
    {
        LineairDB::Protocol::TxRead::Request req;
        req.set_transaction_id(tx_id);
        req.set_table_name(tables::kDistrict);
        req.set_key(make_int_key(w_id, d_id));
        LineairDB::Protocol::TxRead::Response resp;
        if (!client.call(MessageType::TX_READ, req, resp)) return finish_abort();
        if (resp.is_aborted()) return finish_abort();
        if (resp.found() && resp.value().size() >= 4) {
            d_next_o_id = read_int32_le_at(resp.value(), 0);
        }
    }
    // For the last 20 orders × up to 10 order_lines: READ order_line → READ stock
    const int32_t o_lo = d_next_o_id - 20;
    for (int32_t o = o_lo; o < d_next_o_id; ++o) {
        for (int ol = 1; ol <= 10; ++ol) {
            int32_t ol_i_id = 0;
            {
                LineairDB::Protocol::TxRead::Request req;
                req.set_transaction_id(tx_id);
                req.set_table_name(tables::kOrderLine);
                req.set_key(make_int_key(w_id, d_id, o, ol));
                LineairDB::Protocol::TxRead::Response resp;
                if (!client.call(MessageType::TX_READ, req, resp)) return finish_abort();
                if (resp.is_aborted()) return finish_abort();
                if (!resp.found()) continue;
                // ol_i_id isn't encoded in our dummy order_line value; fake it
                // with a hash of (w,d,o,ol) so each order_line reads a different
                // stock row (realistic access pattern even without real data).
                ol_i_id = 1 + static_cast<int32_t>((static_cast<uint64_t>(w_id) * 31ULL
                                                    + static_cast<uint64_t>(d_id) * 101ULL
                                                    + static_cast<uint64_t>(o) * 7919ULL
                                                    + static_cast<uint64_t>(ol) * 997ULL)
                                                   % static_cast<uint64_t>(w.cfg->items));
            }
            {
                LineairDB::Protocol::TxRead::Request req;
                req.set_transaction_id(tx_id);
                req.set_table_name(tables::kStock);
                req.set_key(make_int_key(w_id, ol_i_id));
                LineairDB::Protocol::TxRead::Response resp;
                if (!client.call(MessageType::TX_READ, req, resp)) return finish_abort();
                if (resp.is_aborted()) return finish_abort();
            }
        }
    }

    {
        LineairDB::Protocol::DbEndTransaction::Request req;
        req.set_transaction_id(tx_id);
        req.set_fence(false);
        LineairDB::Protocol::DbEndTransaction::Response resp;
        if (!client.call(MessageType::DB_END_TRANSACTION, req, resp)) return TxOutcome::kAborted;
        if (resp.is_aborted()) return TxOutcome::kAborted;
    }
    return TxOutcome::kCommitted;
}

}  // namespace bench_client
