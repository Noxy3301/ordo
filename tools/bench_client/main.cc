// bench_client driver.
//
// Modes (Step 1/2):
//   --mode roundtrip   : 1 thread BEGIN→END loop, TP report       (default)
//   --mode verify      : 1 iter CREATE_TABLE→BEGIN→SET_TABLE→WRITE→READ→END,
//                        check read-back value matches write
//
// Later steps will add --mode populate, --mode newOrder, and multi-thread.

#include "client.hh"
#include "key_encoder.hh"
#include "lineairdb.pb.h"
#include "stats.hh"
#include "workload_tpcc.hh"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

namespace {

struct Args {
    std::string host = "127.0.0.1";
    int port = 9999;
    int threads = 1;
    double duration_sec = 5.0;
    int iterations = 0;
    std::string mode = "roundtrip";

    // TPC-C populate / newOrder config
    int warehouses = 1;
    int districts_per_warehouse = 10;
    int customers_per_district = 3000;
    int items = 100000;
    int retry_limit = 3;  // BenchBase default

    // Mix selection (weights, summed internally). 0 means disabled.
    // BenchBase TPC-C defaults: 45 / 43 / 4 / 4 / 4.
    // --mix-newOrder / --mix-payment / --mix-orderStatus / --mix-delivery / --mix-stockLevel
    int mix_new_order = 0;
    int mix_payment = 0;
    int mix_order_status = 0;
    int mix_delivery = 0;
    int mix_stock_level = 0;
    // Legacy single-knob for 2-way NewOrder/Payment experiments.
    double payment_ratio = 0.0;
};

void print_usage() {
    std::fprintf(stderr,
                 "bench_client [--host H] [--port P] [--threads N]\n"
                 "             [--duration S] [--iterations N]\n"
                 "             [--mode roundtrip|verify]\n");
}

bool parse_args(int argc, char** argv, Args* a) {
    for (int i = 1; i < argc; ++i) {
        const char* k = argv[i];
        auto next = [&]() -> const char* {
            if (i + 1 >= argc) return nullptr;
            return argv[++i];
        };
        if (std::strcmp(k, "--host") == 0) { const char* v = next(); if (!v) return false; a->host = v; }
        else if (std::strcmp(k, "--port") == 0) { const char* v = next(); if (!v) return false; a->port = std::atoi(v); }
        else if (std::strcmp(k, "--threads") == 0) { const char* v = next(); if (!v) return false; a->threads = std::atoi(v); }
        else if (std::strcmp(k, "--duration") == 0) { const char* v = next(); if (!v) return false; a->duration_sec = std::atof(v); }
        else if (std::strcmp(k, "--iterations") == 0) { const char* v = next(); if (!v) return false; a->iterations = std::atoi(v); }
        else if (std::strcmp(k, "--mode") == 0) { const char* v = next(); if (!v) return false; a->mode = v; }
        else if (std::strcmp(k, "--warehouses") == 0) { const char* v = next(); if (!v) return false; a->warehouses = std::atoi(v); }
        else if (std::strcmp(k, "--districts-per-wh") == 0) { const char* v = next(); if (!v) return false; a->districts_per_warehouse = std::atoi(v); }
        else if (std::strcmp(k, "--customers-per-dist") == 0) { const char* v = next(); if (!v) return false; a->customers_per_district = std::atoi(v); }
        else if (std::strcmp(k, "--items") == 0) { const char* v = next(); if (!v) return false; a->items = std::atoi(v); }
        else if (std::strcmp(k, "--retry-limit") == 0) { const char* v = next(); if (!v) return false; a->retry_limit = std::atoi(v); }
        else if (std::strcmp(k, "--payment-ratio") == 0) { const char* v = next(); if (!v) return false; a->payment_ratio = std::atof(v); }
        else if (std::strcmp(k, "--mix-newOrder") == 0) { const char* v = next(); if (!v) return false; a->mix_new_order = std::atoi(v); }
        else if (std::strcmp(k, "--mix-payment") == 0) { const char* v = next(); if (!v) return false; a->mix_payment = std::atoi(v); }
        else if (std::strcmp(k, "--mix-orderStatus") == 0) { const char* v = next(); if (!v) return false; a->mix_order_status = std::atoi(v); }
        else if (std::strcmp(k, "--mix-delivery") == 0) { const char* v = next(); if (!v) return false; a->mix_delivery = std::atoi(v); }
        else if (std::strcmp(k, "--mix-stockLevel") == 0) { const char* v = next(); if (!v) return false; a->mix_stock_level = std::atoi(v); }
        else { std::fprintf(stderr, "unknown arg: %s\n", k); return false; }
    }
    return true;
}

bool begin_end_once(bench_client::Client& client) {
    LineairDB::Protocol::TxBeginTransaction::Request begin_req;
    LineairDB::Protocol::TxBeginTransaction::Response begin_resp;
    if (!client.call(MessageType::TX_BEGIN_TRANSACTION, begin_req, begin_resp)) return false;

    LineairDB::Protocol::DbEndTransaction::Request end_req;
    end_req.set_transaction_id(begin_resp.transaction_id());
    end_req.set_fence(false);
    LineairDB::Protocol::DbEndTransaction::Response end_resp;
    if (!client.call(MessageType::DB_END_TRANSACTION, end_req, end_resp)) return false;

    return true;
}

int run_roundtrip(const Args& args) {
    bench_client::Client client;
    if (!client.connect(args.host, args.port)) return 2;
    std::printf("connected to %s:%d\n", args.host.c_str(), args.port);

    const auto t0 = std::chrono::steady_clock::now();
    uint64_t done = 0;
    uint64_t failed = 0;

    if (args.iterations > 0) {
        for (int i = 0; i < args.iterations; ++i) {
            if (begin_end_once(client)) ++done; else ++failed;
        }
    } else {
        const auto deadline = t0 + std::chrono::duration<double>(args.duration_sec);
        while (std::chrono::steady_clock::now() < deadline) {
            if (begin_end_once(client)) ++done; else ++failed;
        }
    }

    const double elapsed =
        std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
    const double tp = (elapsed > 0.0) ? static_cast<double>(done) / elapsed : 0.0;
    std::printf("BEGIN->END tx: commits=%lu failed=%lu elapsed=%.3fs TP=%.1f tx/s\n",
                static_cast<unsigned long>(done), static_cast<unsigned long>(failed), elapsed, tp);

    return (failed == 0) ? 0 : 3;
}

// Verify mode: exercise the ops bench_client will use in populate/newOrder.
// Creates a scratch table, writes a key, reads it back, checks equality.
int run_verify(const Args& args) {
    bench_client::Client client;
    if (!client.connect(args.host, args.port)) return 2;
    std::printf("connected to %s:%d\n", args.host.c_str(), args.port);

    const std::string table = "bench_client_verify";
    const std::string key = bench_client::make_int_key(42);
    const std::string value = "hello-bench-client";

    // CREATE_TABLE (idempotent on server side per lineairdb_rpc.cc behavior)
    {
        LineairDB::Protocol::DbCreateTable::Request req;
        req.set_table_name(table);
        LineairDB::Protocol::DbCreateTable::Response resp;
        if (!client.call(MessageType::DB_CREATE_TABLE, req, resp)) {
            std::fprintf(stderr, "CREATE_TABLE failed\n"); return 4;
        }
        std::printf("CREATE_TABLE %s success=%d\n", table.c_str(), resp.success());
    }

    // BEGIN
    int64_t tx_id = 0;
    {
        LineairDB::Protocol::TxBeginTransaction::Request req;
        LineairDB::Protocol::TxBeginTransaction::Response resp;
        if (!client.call(MessageType::TX_BEGIN_TRANSACTION, req, resp)) return 4;
        tx_id = resp.transaction_id();
        std::printf("BEGIN tx_id=%ld\n", static_cast<long>(tx_id));
    }

    // SET_TABLE
    {
        LineairDB::Protocol::DbSetTable::Request req;
        req.set_transaction_id(tx_id);
        req.set_table_name(table);
        LineairDB::Protocol::DbSetTable::Response resp;
        if (!client.call(MessageType::DB_SET_TABLE, req, resp)) return 4;
        if (!resp.success()) { std::fprintf(stderr, "SET_TABLE returned success=false\n"); return 5; }
    }

    // WRITE
    {
        LineairDB::Protocol::TxWrite::Request req;
        req.set_transaction_id(tx_id);
        req.set_table_name(table);
        req.set_key(key);
        req.set_value(value);
        LineairDB::Protocol::TxWrite::Response resp;
        if (!client.call(MessageType::TX_WRITE, req, resp)) return 4;
        if (resp.is_aborted() || !resp.success()) {
            std::fprintf(stderr, "WRITE failed (success=%d aborted=%d)\n", resp.success(), resp.is_aborted());
            return 5;
        }
        std::printf("WRITE key=%zuB value=%zuB success\n", key.size(), value.size());
    }

    // READ (read-your-own-writes)
    {
        LineairDB::Protocol::TxRead::Request req;
        req.set_transaction_id(tx_id);
        req.set_table_name(table);
        req.set_key(key);
        LineairDB::Protocol::TxRead::Response resp;
        if (!client.call(MessageType::TX_READ, req, resp)) return 4;
        if (resp.is_aborted()) { std::fprintf(stderr, "READ aborted\n"); return 5; }
        if (!resp.found()) { std::fprintf(stderr, "READ not found\n"); return 5; }
        if (resp.value() != value) {
            std::fprintf(stderr, "READ mismatch: got %zuB (expect %zuB)\n",
                         resp.value().size(), value.size());
            return 5;
        }
        std::printf("READ found=1 value=%zuB match=OK\n", resp.value().size());
    }

    // END
    {
        LineairDB::Protocol::DbEndTransaction::Request req;
        req.set_transaction_id(tx_id);
        req.set_fence(false);
        LineairDB::Protocol::DbEndTransaction::Response resp;
        if (!client.call(MessageType::DB_END_TRANSACTION, req, resp)) return 4;
        if (resp.is_aborted()) { std::fprintf(stderr, "END aborted\n"); return 5; }
        std::printf("END committed\n");
    }

    std::printf("verify: OK\n");
    return 0;
}

// -----------------------------------------------------------------------------
// newOrder: spawn N worker threads, each on its own connection, run for
// `duration` seconds, aggregate TP/GP/latency.
// -----------------------------------------------------------------------------

int run_new_order(const Args& args) {
    bench_client::TpccConfig cfg{
        args.warehouses, args.districts_per_warehouse,
        args.customers_per_district, args.items};

    std::atomic<bool> stop_flag{false};
    std::vector<bench_client::WorkerStats> per_thread(args.threads);
    std::vector<std::thread> workers;
    workers.reserve(args.threads);

    std::atomic<int> connect_fail{0};

    auto worker_main = [&](int tid) {
        bench_client::Client client;
        if (!client.connect(args.host, args.port)) {
            connect_fail.fetch_add(1, std::memory_order_relaxed);
            return;
        }

        bench_client::NewOrderWorker w{
            tid, &cfg,
            // xorshift64 mustn't start at 0
            static_cast<uint64_t>(0x9E3779B97F4A7C15ULL ^ (static_cast<uint64_t>(tid) + 1) * 1315423911ULL)};

        bench_client::WorkerStats& stats = per_thread[tid];
        stats.latencies_ns.reserve(1 << 15);

        // Per-worker RNG for tx-type selection — independent from the workload
        // RNG so mix probabilities aren't skewed by in-tx consumption.
        uint64_t type_rng = 0xC0FFEE0000000001ULL ^ (static_cast<uint64_t>(tid) + 1) * 2654435761ULL;

        // Resolve tx-selection scheme:
        //   1. If any --mix-* weight is non-zero, use weighted selection across 5 tx types.
        //   2. Else if --payment-ratio > 0, use 2-way NO/Payment split.
        //   3. Else NewOrder only.
        enum TxKind { kNO, kPay, kOS, kDel, kSL };
        const int mix_sum = args.mix_new_order + args.mix_payment +
                            args.mix_order_status + args.mix_delivery + args.mix_stock_level;
        auto pick_tx = [&]() -> TxKind {
            type_rng ^= type_rng >> 12; type_rng ^= type_rng << 25; type_rng ^= type_rng >> 27;
            if (mix_sum > 0) {
                const int r = static_cast<int>(type_rng % static_cast<uint64_t>(mix_sum));
                int acc = 0;
                acc += args.mix_new_order;    if (r < acc) return kNO;
                acc += args.mix_payment;      if (r < acc) return kPay;
                acc += args.mix_order_status; if (r < acc) return kOS;
                acc += args.mix_delivery;     if (r < acc) return kDel;
                return kSL;
            }
            if (args.payment_ratio > 0.0) {
                double u = static_cast<double>(type_rng & 0xFFFFFFFFFFFFULL) / static_cast<double>(1ULL << 48);
                return (u < args.payment_ratio) ? kPay : kNO;
            }
            return kNO;
        };

        while (!stop_flag.load(std::memory_order_relaxed)) {
            const auto t_begin = std::chrono::steady_clock::now();
            const TxKind kind = pick_tx();
            int attempt = 0;
            bench_client::TxOutcome outcome = bench_client::TxOutcome::kAborted;
            for (; attempt <= args.retry_limit; ++attempt) {
                switch (kind) {
                    case kNO:  outcome = bench_client::run_new_order_once(client, w); break;
                    case kPay: outcome = bench_client::run_payment_once(client, w); break;
                    case kOS:  outcome = bench_client::run_order_status_once(client, w); break;
                    case kDel: outcome = bench_client::run_delivery_once(client, w); break;
                    case kSL:  outcome = bench_client::run_stock_level_once(client, w); break;
                }
                if (outcome == bench_client::TxOutcome::kCommitted) break;
                if (attempt < args.retry_limit) {
                    ++stats.retries;  // counts a retry about to happen
                }
            }
            const auto t_end = std::chrono::steady_clock::now();
            const uint64_t ns = static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(t_end - t_begin).count());

            if (outcome == bench_client::TxOutcome::kCommitted) {
                ++stats.commits;
            } else {
                ++stats.aborts_final;
            }
            stats.latencies_ns.push_back(ns);
        }

        client.disconnect();
    };

    const auto t0 = std::chrono::steady_clock::now();
    for (int i = 0; i < args.threads; ++i) {
        workers.emplace_back(worker_main, i);
    }
    std::this_thread::sleep_for(std::chrono::duration<double>(args.duration_sec));
    stop_flag.store(true, std::memory_order_relaxed);
    for (auto& th : workers) th.join();
    const double elapsed =
        std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();

    if (connect_fail.load() > 0) {
        std::fprintf(stderr, "newOrder: %d workers failed to connect\n",
                     connect_fail.load());
    }

    auto agg = bench_client::aggregate(per_thread, elapsed);
    char mix_label[96];
    const int mix_sum = args.mix_new_order + args.mix_payment +
                        args.mix_order_status + args.mix_delivery + args.mix_stock_level;
    if (mix_sum > 0) {
        std::snprintf(mix_label, sizeof(mix_label),
                      "mix=%d/%d/%d/%d/%d",
                      args.mix_new_order, args.mix_payment,
                      args.mix_order_status, args.mix_delivery, args.mix_stock_level);
    } else {
        std::snprintf(mix_label, sizeof(mix_label),
                      "payment_ratio=%.2f", args.payment_ratio);
    }
    std::printf("tpcc[threads=%d wh=%d items=%d %s]: %s\n",
                args.threads, args.warehouses, args.items, mix_label,
                bench_client::format_summary(agg).c_str());
    return 0;
}

}  // namespace

int main(int argc, char** argv) {
    GOOGLE_PROTOBUF_VERIFY_VERSION;

    Args args;
    if (!parse_args(argc, argv, &args)) {
        print_usage();
        return 1;
    }

    int rc = 0;
    if (args.mode == "roundtrip") rc = run_roundtrip(args);
    else if (args.mode == "verify") rc = run_verify(args);
    else if (args.mode == "populate") {
        bench_client::Client client;
        if (!client.connect(args.host, args.port)) rc = 2;
        else {
            bench_client::TpccConfig cfg{
                args.warehouses, args.districts_per_warehouse,
                args.customers_per_district, args.items};
            rc = bench_client::populate(client, cfg) ? 0 : 4;
        }
    }
    else if (args.mode == "newOrder") rc = run_new_order(args);
    else { std::fprintf(stderr, "unknown mode: %s\n", args.mode.c_str()); print_usage(); rc = 1; }

    google::protobuf::ShutdownProtobufLibrary();
    return rc;
}
