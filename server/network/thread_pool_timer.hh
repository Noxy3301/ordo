#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <thread>
#include <vector>

#include "thread_group.hh"

// Single timer thread. On each tick it walks every ThreadGroup and checks
// whether the group has stalled: all workers are doing CPU work but no RPC
// has completed in the last tick.
//
// Modes:
//   ObserveOnly — only write a log line when a stall is detected.
//   Active      — also ask the stalled group to spawn an extra worker.
class ThreadPoolTimer {
public:
    enum class Mode { ObserveOnly, Active };

    explicit ThreadPoolTimer(std::vector<ThreadGroup*> groups, Mode mode = Mode::ObserveOnly);
    ~ThreadPoolTimer();

    // Launch the timer thread. Returns false on thread creation failure.
    bool start();

    // Signal and join the timer thread. Safe to call multiple times.
    void shutdown();

private:
    struct PerGroupState {
        uint32_t stall_streak = 0;
    };

    std::vector<ThreadGroup*> groups_;
    std::vector<PerGroupState> states_;
    Mode mode_;
    std::thread timer_;
    std::atomic<bool> shutdown_{false};
    std::mutex shutdown_mutex_;
    std::condition_variable shutdown_cv_;

    void timer_main();
    void check_stall(PerGroupState& state, ThreadGroup* group);
};
