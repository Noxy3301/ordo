#include "thread_pool_coordinator.hh"

bool ThreadPoolCoordinator::try_reserve_worker() {
    uint32_t cur = total_workers_.load(std::memory_order_relaxed);
    while (cur < max_total_workers_) {
        if (total_workers_.compare_exchange_weak(
                cur, cur + 1,
                std::memory_order_acquire, std::memory_order_relaxed)) {
            return true;
        }
    }
    return false;
}

void ThreadPoolCoordinator::release_worker() {
    total_workers_.fetch_sub(1, std::memory_order_release);
}
