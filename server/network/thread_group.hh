#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>

#include "connection_context.hh"
#include "../storage/database_manager.hh"
#include "message_handler.hh"

// A self-contained epoll-based worker unit. Owns one epoll_fd and one
// worker thread that serves multiple client connections.
// Connections are assigned by TcpServer round-robin at accept time.
class ThreadGroup {
public:
    explicit ThreadGroup(int group_id);
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

private:
    int group_id_;
    int epoll_fd_ = -1;
    std::atomic<bool> shutdown_{false};
    std::thread worker_;
    std::shared_ptr<DatabaseManager> db_manager_;

    std::mutex connections_mutex_;
    std::unordered_map<int, std::unique_ptr<ConnectionContext>> connections_;

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
