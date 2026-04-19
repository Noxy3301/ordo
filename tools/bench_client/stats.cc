#include "stats.hh"

#include <algorithm>
#include <cstdio>

namespace bench_client {

AggregatedStats aggregate(const std::vector<WorkerStats>& per_thread, double elapsed_sec) {
    AggregatedStats out;
    out.elapsed_sec = elapsed_sec;

    size_t total_lat = 0;
    for (const auto& w : per_thread) total_lat += w.latencies_ns.size();
    std::vector<uint64_t> all;
    all.reserve(total_lat);

    for (const auto& w : per_thread) {
        out.commits += w.commits;
        out.retries += w.retries;
        out.aborts_final += w.aborts_final;
        all.insert(all.end(), w.latencies_ns.begin(), w.latencies_ns.end());
    }

    const uint64_t finished = out.commits + out.aborts_final;
    if (elapsed_sec > 0.0) {
        out.tp = static_cast<double>(finished) / elapsed_sec;
        out.gp = static_cast<double>(out.commits) / elapsed_sec;
    }
    if (finished > 0) {
        out.retry_rate = static_cast<double>(out.retries) / static_cast<double>(finished);
    }

    auto pct = [&](double q) -> uint64_t {
        if (all.empty()) return 0;
        size_t idx = static_cast<size_t>(q * (all.size() - 1));
        std::nth_element(all.begin(), all.begin() + idx, all.end());
        return all[idx];
    };
    // Run nth_element in ascending order p50 -> p95 -> p99 to keep partial
    // orderings valid across calls.
    out.p50_ns = pct(0.50);
    out.p95_ns = pct(0.95);
    out.p99_ns = pct(0.99);
    return out;
}

std::string format_summary(const AggregatedStats& s) {
    char buf[512];
    const double p50_ms = static_cast<double>(s.p50_ns) / 1e6;
    const double p95_ms = static_cast<double>(s.p95_ns) / 1e6;
    const double p99_ms = static_cast<double>(s.p99_ns) / 1e6;
    std::snprintf(buf, sizeof(buf),
                  "TP=%8.1f tx/s  GP=%8.1f tx/s  commits=%lu aborts_final=%lu "
                  "retries=%lu retry_rate=%.3f  p50=%.3fms p95=%.3fms p99=%.3fms  "
                  "elapsed=%.3fs",
                  s.tp, s.gp,
                  static_cast<unsigned long>(s.commits),
                  static_cast<unsigned long>(s.aborts_final),
                  static_cast<unsigned long>(s.retries),
                  s.retry_rate, p50_ms, p95_ms, p99_ms, s.elapsed_sec);
    return std::string(buf);
}

}  // namespace bench_client
