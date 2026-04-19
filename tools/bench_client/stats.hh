#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace bench_client {

// Per-thread bench stats, populated by a worker. Merged at end.
struct WorkerStats {
    uint64_t commits = 0;
    uint64_t retries = 0;        // intermediate aborts followed by a retry
    uint64_t aborts_final = 0;   // tx that gave up after retry limit
    std::vector<uint64_t> latencies_ns;  // one entry per finished tx (commit OR abort_final)
};

// Aggregate stats across threads and print a single-line summary.
struct AggregatedStats {
    uint64_t commits = 0;
    uint64_t retries = 0;
    uint64_t aborts_final = 0;
    double elapsed_sec = 0.0;
    double tp = 0.0;      // (commit + abort_final) / elapsed
    double gp = 0.0;      // commit / elapsed
    double retry_rate = 0.0;
    uint64_t p50_ns = 0;
    uint64_t p95_ns = 0;
    uint64_t p99_ns = 0;
};

AggregatedStats aggregate(const std::vector<WorkerStats>& per_thread, double elapsed_sec);
std::string format_summary(const AggregatedStats& s);

}  // namespace bench_client
