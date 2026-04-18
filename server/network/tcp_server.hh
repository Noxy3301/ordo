#pragma once

#include <cstdint>
#include <memory>
#include <vector>

#include "connection_context.hh"
#include "thread_group.hh"
#include "thread_pool_coordinator.hh"
#include "thread_pool_timer.hh"

// Listens on a TCP port and dispatches accepted connections to a fixed
// pool of ThreadGroups (one per hardware thread, round-robin). Subclasses
// supply the per-connection state and the DatabaseManager handle via the
// two virtual hooks below.
class TcpServer {
public:
    TcpServer(uint16_t port = 9999);
    virtual ~TcpServer() = default;

    // Create ThreadGroups, start workers, and run the accept loop.
    // Returns only on fatal startup failure (no graceful shutdown yet).
    void run();

protected:
    // Build the per-connection state object for a newly accepted fd.
    virtual std::unique_ptr<ConnectionContext> create_connection(int fd, int group_id) = 0;

    // Supplied to ThreadGroups so abort_all can call into LineairDB on disconnect.
    virtual std::shared_ptr<DatabaseManager> get_db_manager() = 0;

private:
    uint16_t port_;
    std::unique_ptr<ThreadPoolCoordinator> coordinator_;
    std::vector<std::unique_ptr<ThreadGroup>> thread_groups_;
    std::unique_ptr<ThreadPoolTimer> timer_;

    bool setup_and_listen(int& server_socket);
    void accept_loop(int server_socket);
};
