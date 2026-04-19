#include "workload_tpcc.hh"

#include "key_encoder.hh"
#include "lineairdb.pb.h"

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
    for (int w = 1; w <= cfg.warehouses; ++w) {
        rows.emplace_back(make_int_key(w), dummy_value(value_sizes::kWarehouse, 'w'));
    }
    return flush_batch(c, tables::kWarehouse, rows);
}

bool populate_district(Client& c, const TpccConfig& cfg) {
    std::vector<std::pair<std::string, std::string>> rows;
    // Initial d_next_o_id = 3001 at offset 0 (TPC-C spec after history load).
    std::string v = dummy_value(value_sizes::kDistrict, 'd');
    write_int32_le_at(v, 0, 3001);
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
    const std::string v = dummy_value(value_sizes::kCustomer, 'c');
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
                             tables::kNewOrder, tables::kOrderLine}) {
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
// except for district/stock which carry the RMW-mutated fields.
const std::string& value_of(const char* table) {
    static const std::string v_wh = std::string(value_sizes::kWarehouse, 'w');
    static const std::string v_cu = std::string(value_sizes::kCustomer, 'c');
    static const std::string v_it = std::string(value_sizes::kItem, 'i');
    static const std::string v_oo = std::string(value_sizes::kOorder, 'o');
    static const std::string v_no = std::string(value_sizes::kNewOrder, 'n');
    static const std::string v_ol = std::string(value_sizes::kOrderLine, 'l');
    if (table == tables::kWarehouse) return v_wh;
    if (table == tables::kCustomer) return v_cu;
    if (table == tables::kItem) return v_it;
    if (table == tables::kOorder) return v_oo;
    if (table == tables::kNewOrder) return v_no;
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

}  // namespace bench_client
