#include "tcp_server.hh"
#include "../../common/log.h"

#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <thread>
#include <chrono>
#include <cerrno>
#include <cstring>
#include <atomic>

TcpServer::TcpServer(uint16_t port) : port_(port) {}

void TcpServer::run() {
    LOG_INFO("Starting server on port %d", port_);

    int server_socket;
    if (!setup_and_listen(server_socket)) {
        return;
    }

    // Process-wide worker cap = 4x hardware threads; coordinator enforces it.
    size_t num_groups = std::thread::hardware_concurrency();
    uint32_t max_total_workers = static_cast<uint32_t>(num_groups) * 4;
    coordinator_ = std::make_unique<ThreadPoolCoordinator>(max_total_workers);

    // Create one thread group per hardware thread.
    // unique_ptr members are torn down by ~TcpServer; only close the raw socket here.
    for (size_t i = 0; i < num_groups; i++) {
        auto group = std::make_unique<ThreadGroup>(static_cast<int>(i), coordinator_.get());
        group->set_db_manager(get_db_manager());
        if (!group->start()) {
            LOG_ERROR("Failed to start ThreadGroup %zu, aborting", i);
            close(server_socket);
            return;
        }
        thread_groups_.push_back(std::move(group));
    }

    std::vector<ThreadGroup*> group_ptrs;
    group_ptrs.reserve(thread_groups_.size());
    for (auto& group : thread_groups_) group_ptrs.push_back(group.get());

    timer_ = std::make_unique<ThreadPoolTimer>(std::move(group_ptrs),
                                               ThreadPoolTimer::Mode::Active);
    if (!timer_->start()) {
        LOG_ERROR("Failed to start ThreadPoolTimer, aborting");
        close(server_socket);
        return;
    }

    LOG_INFO("Server listening on port %d with %zu thread groups "
             "(max %u total workers)",
             port_, num_groups, max_total_workers);

    accept_loop(server_socket);

    // Shutdown
    timer_->shutdown();
    for (auto& group : thread_groups_) {
        group->shutdown();
    }
    close(server_socket);
}

bool TcpServer::setup_and_listen(int& server_socket) {
    server_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (server_socket < 0) {
        int err = errno;
        LOG_ERROR("Failed to create socket: %s (errno=%d)", std::strerror(err), err);
        return false;
    }

    int reuse = 1;
    if (setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
        LOG_ERROR("Failed to set SO_REUSEADDR");
        close(server_socket);
        return false;
    }

    struct sockaddr_in server_addr;
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(port_);

    if (bind(server_socket, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        LOG_ERROR("Failed to bind socket");
        close(server_socket);
        return false;
    }

    if (listen(server_socket, 128) < 0) {
        LOG_ERROR("Failed to listen on socket");
        close(server_socket);
        return false;
    }

    return true;
}

void TcpServer::accept_loop(int server_socket) {
    while (true) {
        struct sockaddr_in client_addr;
        socklen_t client_addr_len = sizeof(client_addr);

        int client_socket = accept(server_socket, (struct sockaddr*)&client_addr, &client_addr_len);
        if (client_socket < 0) {
            int err = errno;
            LOG_ERROR("Failed to accept client connection: %s (errno=%d)",
                      std::strerror(err), err);
            if (err == EINTR) continue;
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            continue;
        }

        // Disable Nagle's algorithm for low-latency RPC
        int flag = 1;
        setsockopt(client_socket, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));

        static std::atomic<uint64_t> conn_counter{0};
        int group_id = static_cast<int>(conn_counter.fetch_add(1) % thread_groups_.size());
        auto ctx = create_connection(client_socket, group_id);

        auto client_ip = std::string(inet_ntoa(client_addr.sin_addr));
        LOG_INFO("Accepted connection fd=%d from %s -> group %d",
                 client_socket, client_ip.c_str(), group_id);

        thread_groups_[group_id]->add_connection(client_socket, std::move(ctx));
    }
}
