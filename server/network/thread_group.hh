#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>

#include "connection_context.hh"
#include "../storage/database_manager.hh"
#include "message_handler.hh"
#include "thread_pool_coordinator.hh"

// A self-contained epoll-based worker unit. Owns one epoll_fd and one
// worker thread that serves multiple client connections.
// Connections are assigned by TcpServer round-robin at accept time.
class ThreadGroup {
public:
    ThreadGroup(int group_id, ThreadPoolCoordinator* coordinator);
    ~ThreadGroup();

    // Required for disconnect cleanup: abort_all needs a Database handle.
    void set_db_manager(std::shared_ptr<DatabaseManager> db_manager);

    // Create epoll_fd and spawn the worker. Returns false on failure
    // (caller should unwind and abort server startup).
    bool start();

    // Signal the worker to exit, then close all remaining connections
    // with abort_all cleanup. Safe to call multiple times.
    void shutdown();

    // Register a newly-accepted connection. Sets socket timeouts,
    // stores the context, and arms EPOLLONESHOT on the fd.
    void add_connection(int fd, std::unique_ptr<ConnectionContext> ctx);

    // Observability counters read by a future stall-detection timer. All are
    // single atomics so a reader never needs a cross-counter consistent
    // snapshot.
    uint32_t worker_count() const { return worker_count_.load(std::memory_order_relaxed); }
    uint32_t busy_count() const { return busy_worker_count_.load(std::memory_order_relaxed); }
    uint32_t waiting_count() const { return waiting_worker_count_.load(std::memory_order_relaxed); }
    uint32_t completed_count_reset() {
        return completed_count_.exchange(0, std::memory_order_relaxed);
    }
    int group_id() const { return group_id_; }

private:
    int group_id_;
    ThreadPoolCoordinator* coordinator_;
    int epoll_fd_ = -1;
    int shutdown_event_fd_ = -1;
    std::atomic<bool> shutdown_{false};
    std::thread worker_;
    std::shared_ptr<DatabaseManager> db_manager_;

    std::mutex connections_mutex_;
    std::unordered_map<int, std::unique_ptr<ConnectionContext>> connections_;

    // Total live workers in this group (always 1 for now)
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
    void worker_main();

    // Receive one RPC message, dispatch to the handler, send the response.
    // Returns false on transport failure (client disconnect).
    bool process_one_rpc(ConnectionContext* ctx);

    // Tear down a connection: abort in-flight transactions, deregister
    // from epoll, close the socket, and drop the ConnectionContext.
    void remove_connection(int fd);
};
