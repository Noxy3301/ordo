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

bool create_secondary_index(Client& c, const std::string& table,
                            const std::string& index, uint32_t index_type) {
    LineairDB::Protocol::DbCreateSecondaryIndex::Request req;
    req.set_table_name(table);
    req.set_index_name(index);
    req.set_index_type(index_type);
    LineairDB::Protocol::DbCreateSecondaryIndex::Response resp;
    if (!c.call(MessageType::DB_CREATE_SECONDARY_INDEX, req, resp)) return false;
    // success may be false if already exists; ok.
    return true;
}

// ---------------------------------------------------------------------------
// c_last pool — 10 canned names, assigned deterministically from c_id via
// TPC-C-ish NURand-free mapping. Uniform distribution so ~300 customers share
// each name in a 3000-cust district (creates real SI lookup fan-out).
// ---------------------------------------------------------------------------

constexpr const char* kCustomerLastNames[] = {
    "BARBARBAR", "BARBAROUGHT", "BAROUGHTBAR", "BAROUGHTOUGHT",
    "OUGHTBARBAR", "OUGHTBAROUGHT", "OUGHTOUGHTBAR", "OUGHTOUGHTOUGHT",
    "ABLEABLEABLE", "ABLEABLEBAR",
};
constexpr int kNumLastNames = sizeof(kCustomerLastNames) / sizeof(kCustomerLastNames[0]);

inline const std::string& c_last_for(int c_id) {
    static const std::string pool[kNumLastNames] = {
        kCustomerLastNames[0], kCustomerLastNames[1], kCustomerLastNames[2],
        kCustomerLastNames[3], kCustomerLastNames[4], kCustomerLastNames[5],
        kCustomerLastNames[6], kCustomerLastNames[7], kCustomerLastNames[8],
        kCustomerLastNames[9]};
    return pool[c_id % kNumLastNames];
}

// c_first deterministic from c_id: zero-padded hex so lex order matches c_id.
// Matching the MySQL TPCC DDL `idx_customer_name (c_w_id, c_d_id, c_last, c_first)`
// requires c_first in the SI key; this keeps the 4-part key unique per customer
// even when many customers share a c_last.
inline std::string c_first_for(int c_id) {
    char buf[12];
    std::snprintf(buf, sizeof(buf), "F%08x", c_id);
    return std::string(buf);
}

// Full 4-part SI key (w,d,c_last,c_first). Used for populate (exact insert) and
// for Payment/OrderStatus by-name (3-part prefix scan over this 4-part index).
inline std::string customer_name_si_key(int w_id, int d_id,
                                        const std::string& c_last,
                                        const std::string& c_first) {
    KeyBuilder kb;
    kb.int32(w_id).int32(d_id).string(c_last).string(c_first);
    return kb.take();
}

// 3-part prefix for Payment/OrderStatus by-name lookups — matches what MySQL's
// ha_lineairdb emits on `index_read_map(HA_READ_KEY_EXACT, (w,d,c_last))`.
inline std::string customer_name_si_prefix(int w_id, int d_id,
                                           const std::string& c_last) {
    KeyBuilder kb;
    kb.int32(w_id).int32(d_id).string(c_last);
    return kb.take();
}

// Lexicographic next key of `prefix` (byte-wise +1 with carry). Mirrors
// `proxy/ha_lineairdb.cc::build_prefix_range_end` exactly — used as the
// (exclusive) end key for range-style prefix scans.
inline std::string prefix_end_of(const std::string& prefix) {
    std::string end = prefix;
    for (size_t i = end.size(); i-- > 0;) {
        unsigned char byte = static_cast<unsigned char>(end[i]);
        if (byte != 0xFF) {
            end[i] = static_cast<char>(byte + 1);
            end.resize(i + 1);
            return end;
        }
    }
    return std::string();  // no upper bound
}

// One secondary-index entry queued for a populate batch.
struct SiWriteOp {
    std::string index_name;
    std::string secondary_key;
    std::string primary_key;
};

// Open a batch-write transaction, flush N WriteOps + optional SI writes, commit.
// Returns true if committed (not aborted).
bool flush_batch_with_si(Client& c, const std::string& table,
                         const std::vector<std::pair<std::string, std::string>>& rows,
                         const std::vector<SiWriteOp>& si_rows) {
    if (rows.empty() && si_rows.empty()) return true;

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
    // BATCH_WRITE with PK + SI
    {
        LineairDB::Protocol::TxBatchWrite::Request req;
        req.set_transaction_id(tx_id);
        req.set_table_name(table);
        for (const auto& kv : rows) {
            auto* op = req.add_writes();
            op->set_key(kv.first);
            op->set_value(kv.second);
        }
        for (const auto& si : si_rows) {
            auto* op = req.add_secondary_index_writes();
            op->set_index_name(si.index_name);
            op->set_secondary_key(si.secondary_key);
            op->set_primary_key(si.primary_key);
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

bool flush_batch(Client& c, const std::string& table,
                 const std::vector<std::pair<std::string, std::string>>& rows) {
    return flush_batch_with_si(c, table, rows, {});
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
    std::vector<SiWriteOp> si_rows;
    rows.reserve(kBatchSize);
    si_rows.reserve(kBatchSize);
    // Initial c_balance = -10 at offset 0 (TPC-C spec cents). Payment RMW target.
    std::string v = dummy_value(value_sizes::kCustomer, 'c');
    write_int32_le_at(v, 0, -10);
    for (int w = 1; w <= cfg.warehouses; ++w) {
        for (int d = 1; d <= cfg.districts_per_warehouse; ++d) {
            for (int ci = 1; ci <= cfg.customers_per_district; ++ci) {
                const std::string pk = make_int_key(w, d, ci);
                rows.emplace_back(pk, v);
                // 4-part SI key (w,d,c_last,c_first) matches MySQL TPCC DDL.
                // Prefix-scan on (w,d,c_last) returns all customers sharing that
                // c_last, lex-ordered by c_first.
                si_rows.push_back(SiWriteOp{
                    indexes::kIdxCustomerName,
                    customer_name_si_key(w, d, c_last_for(ci), c_first_for(ci)),
                    pk});
                if (static_cast<int>(rows.size()) >= kBatchSize) {
                    if (!flush_batch_with_si(c, tables::kCustomer, rows, si_rows))
                        return false;
                    rows.clear();
                    si_rows.clear();
                }
            }
        }
    }
    return flush_batch_with_si(c, tables::kCustomer, rows, si_rows);
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
    // idx_customer_name is non-unique (type=0). Multiple customers share a c_last,
    // so each SI entry accumulates a list of primary keys.
    if (!create_secondary_index(client, tables::kCustomer,
                                indexes::kIdxCustomerName, 0)) {
        std::fprintf(stderr, "populate: CREATE_SECONDARY_INDEX customer failed\n");
        return false;
    }
    // idx_oorder_customer: non-unique SI on (w, d, c_id). Populated during
    // NewOrder runtime so the measured phase drives WriteSecondaryIndex.
    if (!create_secondary_index(client, tables::kOorder,
                                indexes::kIdxOorderCustomer, 0)) {
        std::fprintf(stderr, "populate: CREATE_SECONDARY_INDEX oorder failed\n");
        return false;
    }
    std::printf("populate: tables + secondary indexes created\n");

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

    // BATCH_WRITE [oorder (w,d,o_id)] + SI [idx_oorder_customer (w,d,c_id) → oorder PK]
    // then [new_order (w,d,o_id)]. The oorder SI write exercises the LDB
    // WriteSecondaryIndex hotpath on every NewOrder commit (45% of mix).
    {
        LineairDB::Protocol::TxBatchWrite::Request req;
        req.set_transaction_id(tx_id);
        req.set_table_name(tables::kOorder);
        const std::string oorder_pk = make_int_key(w_id, d_id, o_id);
        auto* op = req.add_writes();
        op->set_key(oorder_pk);
        op->set_value(value_of(tables::kOorder));
        auto* si = req.add_secondary_index_writes();
        si->set_index_name(indexes::kIdxOorderCustomer);
        si->set_secondary_key(make_int_key(w_id, d_id, c_id));
        si->set_primary_key(oorder_pk);
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
    const int32_t h_amount = 1 + static_cast<int32_t>(xorshift64(w.rng_state) % 5000);
    // TPC-C §2.5.1.2: 60% of Payments resolve customer by last name (SI lookup),
    // 40% by primary key. The by-name path exercises TxReadSecondaryIndex and
    // the customer-by-last-name MPMC.
    const bool by_name = (xorshift64(w.rng_state) % 100) < 60;

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

    // Resolve customer PK. 60% by c_last via SI prefix scan (TPC-C §2.5.1.2):
    //   - TX_GET_MATCHING_PRIMARY_KEYS_FROM_PREFIX on idx_customer_name with
    //     3-part prefix (w,d,c_last). Returns all customers with that c_last
    //     in that district, lex-ordered by c_first (what MySQL sees after
    //     index_read_map + index_next iteration).
    //   - TX_READ every returned PK — matches MySQL's ResultSet.next() loop
    //     fetching full rows via rnd_pos. ~300 reads per by-name Payment.
    //   - Pick the (n+1)/2-th row by c_first order = middle of the list.
    std::string cu_key;
    if (by_name) {
        const std::string& c_last = kCustomerLastNames[
            xorshift64(w.rng_state) % kNumLastNames];
        std::vector<std::string> pks;
        {
            // Matches proxy path: kSameKeyMaterialize / kPrefixFirst both use
            // get_matching_primary_keys_in_range (ha_lineairdb.cc:2468,2516)
            // with start=prefix, end=exclusive_end=prefix_end_of(prefix).
            const std::string prefix = customer_name_si_prefix(w_id, d_id, c_last);
            const std::string pend = prefix_end_of(prefix);
            LineairDB::Protocol::TxGetMatchingPrimaryKeysInRange::Request req;
            req.set_transaction_id(tx_id);
            req.set_table_name(tables::kCustomer);
            req.set_index_name(indexes::kIdxCustomerName);
            req.set_start_key(prefix);
            req.set_end_key(pend);
            req.set_exclusive_end_key(pend);
            LineairDB::Protocol::TxGetMatchingPrimaryKeysInRange::Response resp;
            if (!client.call(MessageType::TX_GET_MATCHING_PRIMARY_KEYS_IN_RANGE, req, resp))
                return finish_abort();
            if (resp.is_aborted()) return finish_abort();
            if (resp.primary_keys_size() == 0) return finish_abort();
            pks.reserve(resp.primary_keys_size());
            for (const auto& pk : resp.primary_keys()) pks.push_back(pk);
        }
        // Batch-fetch every matching customer row in a single RPC. Mirrors the
        // MySQL proxy's `batch_fetch_secondary_payloads` (ha_lineairdb.cc:2696)
        // which issues ONE TX_BATCH_READ for all SI-returned PKs — NOT N
        // individual reads. All rows enter read_set_ for validation.
        {
            LineairDB::Protocol::TxBatchRead::Request req;
            req.set_transaction_id(tx_id);
            req.set_table_name(tables::kCustomer);
            for (const auto& pk : pks) req.add_keys(pk);
            LineairDB::Protocol::TxBatchRead::Response resp;
            if (!client.call(MessageType::TX_BATCH_READ, req, resp)) return finish_abort();
            if (resp.is_aborted()) return finish_abort();
        }
        cu_key = pks[pks.size() / 2];
    } else {
        const int c_id = 1 + rand_range(w.rng_state, 0, w.cfg->customers_per_district - 1);
        cu_key = make_int_key(w_id, d_id, c_id);
    }
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
// OrderStatus execution. TPC-C §2.6:
//   - 60% resolve customer by c_last (SI lookup); 40% by c_id
//   - find latest oorder for the customer → in our bench we use TX_FETCH_LAST_KEY_IN_RANGE
//     over the district's oorder range (approximation: latest order in district,
//     not latest for the specific customer — avoids needing a customer→order SI)
//   - read order_line via TX_GET_MATCHING_KEYS_AND_VALUES_FROM_PREFIX
// -----------------------------------------------------------------------------

TxOutcome run_order_status_once(Client& client, NewOrderWorker& w) {
    const int w_id = 1 + rand_range(w.rng_state, 0, w.cfg->warehouses - 1);
    const int d_id = 1 + rand_range(w.rng_state, 0, w.cfg->districts_per_warehouse - 1);
    const bool by_name = (xorshift64(w.rng_state) % 100) < 60;

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

    // Resolve customer. 60% via SI prefix scan (same MySQL-equivalent path as
    // Payment): 3-part prefix (w,d,c_last) → list of matching PKs → read each
    // row → pick middle. Scan response is a protobuf list (not flat binary).
    std::string cu_key;
    if (by_name) {
        const std::string& c_last = kCustomerLastNames[
            xorshift64(w.rng_state) % kNumLastNames];
        std::vector<std::string> pks;
        {
            // Matches proxy path: kSameKeyMaterialize / kPrefixFirst both use
            // get_matching_primary_keys_in_range (ha_lineairdb.cc:2468,2516)
            // with start=prefix, end=exclusive_end=prefix_end_of(prefix).
            const std::string prefix = customer_name_si_prefix(w_id, d_id, c_last);
            const std::string pend = prefix_end_of(prefix);
            LineairDB::Protocol::TxGetMatchingPrimaryKeysInRange::Request req;
            req.set_transaction_id(tx_id);
            req.set_table_name(tables::kCustomer);
            req.set_index_name(indexes::kIdxCustomerName);
            req.set_start_key(prefix);
            req.set_end_key(pend);
            req.set_exclusive_end_key(pend);
            LineairDB::Protocol::TxGetMatchingPrimaryKeysInRange::Response resp;
            if (!client.call(MessageType::TX_GET_MATCHING_PRIMARY_KEYS_IN_RANGE, req, resp))
                return finish_abort();
            if (resp.is_aborted()) return finish_abort();
            if (resp.primary_keys_size() == 0) return finish_abort();
            pks.reserve(resp.primary_keys_size());
            for (const auto& pk : resp.primary_keys()) pks.push_back(pk);
        }
        for (const auto& pk : pks) {
            LineairDB::Protocol::TxRead::Request req;
            req.set_transaction_id(tx_id);
            req.set_table_name(tables::kCustomer);
            req.set_key(pk);
            LineairDB::Protocol::TxRead::Response resp;
            if (!client.call(MessageType::TX_READ, req, resp)) return finish_abort();
            if (resp.is_aborted()) return finish_abort();
        }
        cu_key = pks[pks.size() / 2];
    } else {
        const int c_id = 1 + rand_range(w.rng_state, 0, w.cfg->customers_per_district - 1);
        cu_key = make_int_key(w_id, d_id, c_id);
    }

    // customer READ
    {
        LineairDB::Protocol::TxRead::Request req;
        req.set_transaction_id(tx_id);
        req.set_table_name(tables::kCustomer);
        req.set_key(cu_key);
        LineairDB::Protocol::TxRead::Response resp;
        if (!client.call(MessageType::TX_READ, req, resp)) return finish_abort();
        if (resp.is_aborted()) return finish_abort();
    }

    // Resolve c_id from cu_key (3rd int32 part) so we can look up the customer's
    // own orders via idx_oorder_customer instead of district-latest. Each key
    // part is 8 bytes; 3rd part payload is at offset 20..23.
    int32_t c_id = 0;
    if (cu_key.size() >= 3 * 8) {
        uint32_t be = 0;
        be |= static_cast<uint8_t>(cu_key[20]); be <<= 8;
        be |= static_cast<uint8_t>(cu_key[21]); be <<= 8;
        be |= static_cast<uint8_t>(cu_key[22]); be <<= 8;
        be |= static_cast<uint8_t>(cu_key[23]);
        c_id = static_cast<int32_t>(be ^ 0x80000000u);
    }

    // Fetch this customer's oorders via idx_oorder_customer (w,d,c_id). Picks
    // the last in the returned list as the "latest" (SI accumulates in insertion
    // order). If SI lookup returns empty (customer has no orders yet), commit.
    std::string latest_o_key;
    {
        LineairDB::Protocol::TxReadSecondaryIndex::Request req;
        req.set_transaction_id(tx_id);
        req.set_table_name(tables::kOorder);
        req.set_index_name(indexes::kIdxOorderCustomer);
        req.set_secondary_key(make_int_key(w_id, d_id, c_id));
        LineairDB::Protocol::TxReadSecondaryIndex::Response resp;
        if (!client.call(MessageType::TX_READ_SECONDARY_INDEX, req, resp))
            return finish_abort();
        if (resp.is_aborted()) return finish_abort();
        if (resp.values_size() == 0) {
            LineairDB::Protocol::DbEndTransaction::Request end_req;
            end_req.set_transaction_id(tx_id);
            end_req.set_fence(false);
            LineairDB::Protocol::DbEndTransaction::Response end_resp;
            if (!client.call(MessageType::DB_END_TRANSACTION, end_req, end_resp)) return TxOutcome::kAborted;
            if (end_resp.is_aborted()) return TxOutcome::kAborted;
            return TxOutcome::kCommitted;
        }
        latest_o_key = resp.values(resp.values_size() - 1);
    }

    // READ that oorder by its PK (registers in read_set_ for validation).
    {
        LineairDB::Protocol::TxRead::Request req;
        req.set_transaction_id(tx_id);
        req.set_table_name(tables::kOorder);
        req.set_key(latest_o_key);
        LineairDB::Protocol::TxRead::Response resp;
        if (!client.call(MessageType::TX_READ, req, resp)) return finish_abort();
        if (resp.is_aborted()) return finish_abort();
    }

    // Scan all order_lines for this order (prefix scan). The response is flat
    // binary format (not protobuf), emitted by handleTxGetMatchingKeysAndValuesFromPrefix.
    {
        LineairDB::Protocol::TxGetMatchingKeysAndValuesFromPrefix::Request req;
        req.set_transaction_id(tx_id);
        req.set_table_name(tables::kOrderLine);
        req.set_prefix(latest_o_key);   // (w,d,o_id) is the order_line PK prefix
        bool aborted = false;
        std::vector<std::pair<std::string, std::string>> kvs;
        if (!client.call_flat_scan(MessageType::TX_GET_MATCHING_KEYS_AND_VALUES_FROM_PREFIX,
                                   req, &aborted, &kvs))
            return finish_abort();
        if (aborted) return finish_abort();
        (void)kvs.size();
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
// Config sanity bounds for Delivery loop — bench_client targets SF=1 so 10
// districts is the expected upper bound; the constant guards against misconfig.
constexpr int kMaxWh = 64;
constexpr int kMaxDi = 64;
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
        // TPC-C §2.7: SELECT MIN(no_o_id) FROM new_order WHERE (w,d) ORDER BY ASC LIMIT 1.
        // At RPC level: TX_FETCH_FIRST_KEY_WITH_PREFIX, prefix=(w,d), prefix_end=(w,d+1).
        int32_t o_id = 0;
        {
            LineairDB::Protocol::TxFetchFirstKeyWithPrefix::Request req;
            req.set_transaction_id(tx_id);
            req.set_table_name(tables::kNewOrder);
            req.set_prefix(make_int_key(w_id, d_id));
            req.set_prefix_end(make_int_key(w_id, d_id + 1));
            LineairDB::Protocol::TxFetchFirstKeyWithPrefix::Response resp;
            if (!client.call(MessageType::TX_FETCH_FIRST_KEY_WITH_PREFIX, req, resp))
                return finish_abort();
            if (resp.is_aborted()) return finish_abort();
            if (!resp.found()) continue;  // queue empty for this district

            // Decode o_id from the 3rd int32 key-part. Format per part:
            // [1B null][1B type=0x10][2B len=0x0004][4B sign-flipped BE int32].
            const std::string& k = resp.key();
            if (k.size() < 3 * 8) return finish_abort();
            // offset of 3rd part payload = 2*8 + 4 = 20; length 4.
            uint32_t be = 0;
            be |= static_cast<uint8_t>(k[20]); be <<= 8;
            be |= static_cast<uint8_t>(k[21]); be <<= 8;
            be |= static_cast<uint8_t>(k[22]); be <<= 8;
            be |= static_cast<uint8_t>(k[23]);
            o_id = static_cast<int32_t>(be ^ 0x80000000u);  // undo sign-flip
        }

        // oorder sanity check (should exist if new_order was drained).
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

        // new_order DELETE via dedicated TX_DELETE (tombstone with TID bump).
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

    // District READ to find d_next_o_id.
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

    // Range-scan order_line where (w,d,o_id) ∈ [d_next-20, d_next). This is the
    // primary PL (precision_locking) exerciser: the scan registers a predicate
    // so concurrent NewOrder inserting new order_lines into this range can abort.
    // Response is flat binary (not protobuf).
    std::vector<int32_t> i_ids;
    i_ids.reserve(200);
    {
        const int32_t o_lo = (d_next_o_id > 20 + 3001) ? (d_next_o_id - 20) : 3001;
        const int32_t o_hi = d_next_o_id;  // TPC-C §2.8.2.2: ol_o_id < d_next_o_id
        LineairDB::Protocol::TxGetMatchingKeysAndValuesInRange::Request req;
        req.set_transaction_id(tx_id);
        req.set_table_name(tables::kOrderLine);
        req.set_start_key(make_int_key(w_id, d_id, o_lo));
        // LDB scan end_key is inclusive; use exclusive_end_key to enforce `< o_hi`.
        req.set_end_key(make_int_key(w_id, d_id, o_hi));
        req.set_exclusive_end_key(make_int_key(w_id, d_id, o_hi));
        bool aborted = false;
        std::vector<std::pair<std::string, std::string>> kvs;
        if (!client.call_flat_scan(MessageType::TX_GET_MATCHING_KEYS_AND_VALUES_IN_RANGE,
                                   req, &aborted, &kvs))
            return finish_abort();
        if (aborted) return finish_abort();
        // Derive a plausible ol_i_id per returned order_line for stock reads.
        for (const auto& kv : kvs) {
            uint64_t h = 1469598103934665603ULL;  // FNV offset
            for (unsigned char c : kv.first) {
                h = (h ^ c) * 1099511628211ULL;
            }
            i_ids.push_back(1 + static_cast<int32_t>(h % static_cast<uint64_t>(w.cfg->items)));
        }
    }

    // For each ol_i_id: READ stock. This exercises the stock-primary MPMC path
    // under the umbrella of a tx that ALSO registered a PL predicate above.
    for (int32_t i_id : i_ids) {
        LineairDB::Protocol::TxRead::Request req;
        req.set_transaction_id(tx_id);
        req.set_table_name(tables::kStock);
        req.set_key(make_int_key(w_id, i_id));
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

}  // namespace bench_client
