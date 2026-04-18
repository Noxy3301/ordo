#include "thread_pool_timer.hh"
#include "../../common/log.h"

#include <chrono>

// Interval between stall checks.
static constexpr auto STALL_TICK = std::chrono::milliseconds(50);

// Number of consecutive stall ticks required before firing. Filters out
// transient busy spikes that resolve on the next tick.
static constexpr uint32_t STALL_STREAK_THRESHOLD = 2;

ThreadPoolTimer::ThreadPoolTimer(std::vector<ThreadGroup*> groups, Mode mode)
    : groups_(std::move(groups)), states_(groups_.size()), mode_(mode) {}

ThreadPoolTimer::~ThreadPoolTimer() {
    shutdown();
}

bool ThreadPoolTimer::start() {
    try {
        timer_ = std::thread(&ThreadPoolTimer::timer_main, this);
    } catch (...) {
        LOG_ERROR("ThreadPoolTimer: thread creation failed");
        return false;
    }
    return true;
}

void ThreadPoolTimer::shutdown() {
    {
        std::lock_guard<std::mutex> lk(shutdown_mutex_);
        shutdown_.store(true, std::memory_order_release);
    }
    shutdown_cv_.notify_all();
    if (timer_.joinable()) {
        timer_.join();
    }
}

void ThreadPoolTimer::timer_main() {
    while (!shutdown_.load(std::memory_order_acquire)) {
        {
            std::unique_lock<std::mutex> lk(shutdown_mutex_);
            shutdown_cv_.wait_for(lk, STALL_TICK,
                [this] { return shutdown_.load(std::memory_order_acquire); });
        }
        if (shutdown_.load(std::memory_order_acquire)) break;
        for (size_t i = 0; i < groups_.size(); i++) {
            check_stall(states_[i], groups_[i]);
        }
    }
}

void ThreadPoolTimer::check_stall(PerGroupState& state, ThreadGroup* group) {
    uint32_t completed = group->completed_count_reset();
    uint32_t total = group->worker_count();
    uint32_t busy = group->busy_count();
    uint32_t waiting = group->waiting_count();

    // A group is stalled when every live worker is doing CPU work and none
    // of them finished an RPC in this tick. Recv-blocked workers don't count
    // — they are just waiting on the client, so spawning another worker
    // would not help.
    const bool is_stall = (total >= 1)
                       && (busy >= total)
                       && (waiting == 0)
                       && (completed == 0);

    if (!is_stall) {
        state.stall_streak = 0;
        return;
    }
    if (++state.stall_streak < STALL_STREAK_THRESHOLD) return;

    LOG_INFO("ThreadPoolTimer: stall detected on group %d "
             "(streak=%u, busy=%u/%u, completed=%u)",
             group->group_id(), state.stall_streak, busy, total, completed);
    // Active mode will ask the group to spawn an extra worker here; that
    // hook lands in a later commit.
    state.stall_streak = 0;
}
