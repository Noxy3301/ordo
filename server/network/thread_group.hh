#pragma once

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

#include "connection_context.hh"
#include "../storage/database_manager.hh"
#include "message_handler.hh"
#include "thread_pool_coordinator.hh"

// A self-contained epoll-based worker unit. Owns one epoll_fd and one or
// more worker threads that serve multiple client connections. Extra workers
// are spawned dynamically by ThreadPoolTimer when a stall is detected.
// Connections are assigned by TcpServer round-robin at accept time.
class ThreadGroup {
public:
    ThreadGroup(int group_id, ThreadPoolCoordinator* coordinator);
    ~ThreadGroup();

    // Required for disconnect cleanup: abort_all needs a Database handle.
    void set_db_manager(std::shared_ptr<DatabaseManager> db_manager);

    // Create epoll_fd and spawn the initial worker. Returns false on failure
    // (caller should unwind and abort server startup).
    bool start();

    // Signal all workers to exit, then close all remaining connections
    // with abort_all cleanup. Safe to call multiple times.
    void shutdown();

    // Register a newly-accepted connection. Sets socket timeouts,
    // stores the context, and arms EPOLLONESHOT on the fd.
    void add_connection(int fd, std::unique_ptr<ConnectionContext> ctx);

    // Observability counters read by the stall-detection timer. All are
    // single atomics so a reader never needs a cross-counter consistent
    // snapshot.
    uint32_t worker_count() const { return worker_count_.load(std::memory_order_relaxed); }
    uint32_t busy_count() const { return busy_worker_count_.load(std::memory_order_relaxed); }
    uint32_t waiting_count() const { return waiting_worker_count_.load(std::memory_order_relaxed); }
    uint32_t completed_count_reset() {
        return completed_count_.exchange(0, std::memory_order_relaxed);
    }
    int group_id() const { return group_id_; }

    // Try to spawn an extra worker. Called by ThreadPoolTimer in active
    // mode on stall. Subject to per-group cap, global cap (via
    // coordinator), and a Percona-style throttle.
    void maybe_spawn_worker();

private:
    // One ThreadGroup worker thread + a retirement flag. The worker flips
    // retired to true on exit; cleanup_retired_unlocked() joins and erases
    // such entries during normal operation (not only at shutdown).
    struct WorkerEntry {
        std::thread thread;
        std::atomic<bool> retired{false};
    };

    int group_id_;
    ThreadPoolCoordinator* coordinator_;
    int epoll_fd_ = -1;
    int shutdown_event_fd_ = -1;
    std::atomic<bool> shutdown_{false};
    std::shared_ptr<DatabaseManager> db_manager_;

    std::mutex connections_mutex_;
    std::unordered_map<int, std::unique_ptr<ConnectionContext>> connections_;

    // Workers: *_unlocked methods require workers_mutex_ to be held by the
    // caller. maybe_spawn_worker() is the public entry point and takes the
    // mutex internally.
    std::mutex workers_mutex_;
    std::vector<std::unique_ptr<WorkerEntry>> workers_;
    std::chrono::steady_clock::time_point last_spawn_time_;

    // Total live workers in this group
    std::atomic<uint32_t> worker_count_{0};

    // Workers currently running handle_rpc + send_response; the stall predicate's
    // "occupancy" signal
    std::atomic<uint32_t> busy_worker_count_{0};

    // Workers currently blocked in recv waiting for the client; diagnostics only
    std::atomic<uint32_t> waiting_worker_count_{0};

    // RPC handler completions since the last reset; the stall predicate's
    // "progress" signal (busy without progress for a tick window = stall)
    std::atomic<uint32_t> completed_count_{0};

    // Main epoll loop: wait for events, dispatch to process_one_rpc,
    // and re-arm EPOLLONESHOT. Exits when shutdown_ is set.
    void worker_main(WorkerEntry* self);

    // Receive one RPC message, dispatch to the handler, send the response.
    // Returns false on transport failure (client disconnect).
    bool process_one_rpc(ConnectionContext* ctx);

    // Tear down a connection: abort in-flight transactions, deregister
    // from epoll, close the socket, and drop the ConnectionContext.
    void remove_connection(int fd);

    // Create a new worker thread and add it to workers_. Returns false if
    // the coordinator has no free slot. Requires workers_mutex_.
    bool spawn_worker_unlocked();

    // Join and erase worker entries that have already exited (retired=true).
    // Requires workers_mutex_.
    void cleanup_retired_unlocked();

    // Decide whether the calling worker should exit. Decrements
    // worker_count_ via CAS only when > 1 worker exists so the baseline
    // worker is always preserved. Sets *retired=true on CAS success.
    bool try_retire_local(bool* retired);
};
