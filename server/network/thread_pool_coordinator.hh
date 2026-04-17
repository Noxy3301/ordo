#pragma once

#include <atomic>
#include <cstdint>

// Process-wide cap on the number of worker threads across all ThreadGroups.
// Each ThreadGroup asks for a slot via try_reserve_worker() before spawning
// a new worker thread and returns it via release_worker() when the worker exits.
class ThreadPoolCoordinator {
public:
    explicit ThreadPoolCoordinator(uint32_t max_total_workers)
        : max_total_workers_(max_total_workers) {}

    // Reserve a worker slot. Returns false if the cap would be exceeded.
    // On success, the caller must eventually call release_worker() to return it.
    bool try_reserve_worker();

    // Return a previously reserved worker slot. Must balance each successful
    // try_reserve_worker() call.
    void release_worker();

    uint32_t total_workers() const { return total_workers_.load(std::memory_order_relaxed); }
    uint32_t max_total_workers() const { return max_total_workers_; }

private:
    const uint32_t max_total_workers_;
    std::atomic<uint32_t> total_workers_{0};
};
