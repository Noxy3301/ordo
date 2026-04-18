#include "thread_group.hh"
#include "../../common/log.h"

#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>
#include <cerrno>
#include <cstring>
#include <stdexcept>

static constexpr int MAX_EVENTS = 64;
static constexpr int EPOLL_TIMEOUT_MS = 1000;
static constexpr int SOCKET_TIMEOUT_SEC = 30;

ThreadGroup::ThreadGroup(int group_id) : group_id_(group_id) {}

ThreadGroup::~ThreadGroup() {
    shutdown();
}

void ThreadGroup::set_db_manager(std::shared_ptr<DatabaseManager> db_manager) {
    db_manager_ = std::move(db_manager);
}

bool ThreadGroup::start() {
    epoll_fd_ = epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd_ < 0) {
        LOG_ERROR("ThreadGroup %d: epoll_create1 failed: %s", group_id_, std::strerror(errno));
        return false;
    }

    // eventfd: written by shutdown() to wake the worker out of epoll_wait
    // immediately. Workers blocked in recv() still need SO_RCVTIMEO or a
    // peer close.
    shutdown_event_fd_ = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (shutdown_event_fd_ < 0) {
        LOG_ERROR("ThreadGroup %d: eventfd failed: %s", group_id_, std::strerror(errno));
        close(epoll_fd_);
        epoll_fd_ = -1;
        return false;
    }
    struct epoll_event ev{};
    ev.events = EPOLLIN;
    ev.data.fd = shutdown_event_fd_;
    if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, shutdown_event_fd_, &ev) < 0) {
        LOG_ERROR("ThreadGroup %d: epoll_ctl ADD shutdown_event_fd failed: %s",
                  group_id_, std::strerror(errno));
        close(shutdown_event_fd_); shutdown_event_fd_ = -1;
        close(epoll_fd_); epoll_fd_ = -1;
        return false;
    }

    worker_count_.fetch_add(1, std::memory_order_relaxed);
    try {
        worker_ = std::thread(&ThreadGroup::worker_main, this);
    } catch (...) {
        LOG_ERROR("ThreadGroup %d: thread creation failed", group_id_);
        worker_count_.fetch_sub(1, std::memory_order_relaxed);
        close(shutdown_event_fd_); shutdown_event_fd_ = -1;
        close(epoll_fd_); epoll_fd_ = -1;
        return false;
    }
    return true;
}

void ThreadGroup::shutdown() {
    shutdown_.store(true, std::memory_order_release);

    // Wake the worker if it is sleeping in epoll_wait. Workers blocked in
    // recv() wait for SO_RCVTIMEO (30s) or a peer close — eventfd cannot
    // unblock recv.
    if (shutdown_event_fd_ >= 0) {
        uint64_t v = 1;
        ssize_t r = write(shutdown_event_fd_, &v, sizeof(v));
        (void)r;  // silence -Wunused-result
    }

    if (worker_.joinable()) {
        worker_.join();
    }

    // Close all remaining connections
    std::lock_guard<std::mutex> lk(connections_mutex_);
    for (auto& [fd, ctx] : connections_) {
        if (db_manager_ && ctx->tx_manager) {
            ctx->tx_manager->abort_all(db_manager_->get_database().get());
        }
        close(fd);
    }
    connections_.clear();

    if (shutdown_event_fd_ >= 0) {
        close(shutdown_event_fd_);
        shutdown_event_fd_ = -1;
    }
    if (epoll_fd_ >= 0) {
        close(epoll_fd_);
        epoll_fd_ = -1;
    }
}

void ThreadGroup::add_connection(int fd, std::unique_ptr<ConnectionContext> ctx) {
    // Set socket timeouts for stall protection
    struct timeval tv;
    tv.tv_sec = SOCKET_TIMEOUT_SEC;
    tv.tv_usec = 0;
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

    {
        std::lock_guard<std::mutex> lk(connections_mutex_);
        connections_[fd] = std::move(ctx);
    }

    struct epoll_event ev;
    ev.events = EPOLLIN | EPOLLONESHOT;
    ev.data.fd = fd;
    if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &ev) < 0) {
        LOG_ERROR("ThreadGroup %d: epoll_ctl ADD fd=%d failed: %s",
                  group_id_, fd, std::strerror(errno));
        remove_connection(fd);
    }
}

void ThreadGroup::worker_main() {
    LOG_INFO("ThreadGroup %d: worker started", group_id_);

    struct epoll_event events[MAX_EVENTS];

    while (!shutdown_.load(std::memory_order_relaxed)) {
        int n = epoll_wait(epoll_fd_, events, MAX_EVENTS, EPOLL_TIMEOUT_MS);
        if (n < 0) {
            if (errno == EINTR) continue;
            LOG_ERROR("ThreadGroup %d: epoll_wait failed: %s",
                      group_id_, std::strerror(errno));
            break;
        }
        if (n == 0) continue;  // timeout, re-check shutdown flag

        for (int i = 0; i < n; i++) {
            int fd = events[i].data.fd;
            if (fd == shutdown_event_fd_) {
                shutdown_.store(true, std::memory_order_release);
                break;
            }

            ConnectionContext* ctx = nullptr;
            {
                std::lock_guard<std::mutex> lk(connections_mutex_);
                auto it = connections_.find(fd);
                if (it == connections_.end()) continue;
                ctx = it->second.get();
            }

            // Transaction pinning: while the connection has an active tx,
            // keep processing RPCs from this connection without returning to
            // epoll. This ensures one-worker-one-active-tx (thread affinity
            // required by LineairDB's epoch framework).
            bool alive = true;
            do {
                if (!process_one_rpc(ctx)) {
                    alive = false;
                    break;
                }
            } while (ctx->tx_manager->has_active_transactions());

            if (alive) {
                // No active tx — re-arm EPOLLONESHOT for next message
                struct epoll_event ev;
                ev.events = EPOLLIN | EPOLLONESHOT;
                ev.data.fd = fd;
                if (epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &ev) < 0) {
                    LOG_ERROR("ThreadGroup %d: epoll_ctl MOD fd=%d failed: %s",
                              group_id_, fd, std::strerror(errno));
                    remove_connection(fd);
                }
            } else {
                remove_connection(fd);
            }
        }
    }

    worker_count_.fetch_sub(1, std::memory_order_relaxed);
    LOG_INFO("ThreadGroup %d: worker stopped", group_id_);
}

bool ThreadGroup::process_one_rpc(ConnectionContext* ctx) {
    uint64_t sender_id;
    MessageType message_type;
    std::string payload;

    // Phase 1: blocking recv. Not productive server-side work — the worker is
    // waiting for the client. Counted as "waiting" only.
    waiting_worker_count_.fetch_add(1, std::memory_order_relaxed);
    bool ok = MessageHandler::receive_message(ctx->fd, sender_id, message_type, payload);
    waiting_worker_count_.fetch_sub(1, std::memory_order_relaxed);
    if (!ok) return false;

    // Phase 2: server-side work (handler + response). Counted as "busy";
    // stall detection looks at this counter.
    busy_worker_count_.fetch_add(1, std::memory_order_relaxed);
    std::string result;
    ctx->rpc_handler->handle_rpc(sender_id, message_type, payload, result);

    // Progress metric: RPC handler completed. Represents handler progress,
    // not DB commit/abort status.
    completed_count_.fetch_add(1, std::memory_order_relaxed);

    bool sent = MessageHandler::send_response_writev(ctx->fd, 0, message_type, result);
    busy_worker_count_.fetch_sub(1, std::memory_order_relaxed);
    return sent;
}

void ThreadGroup::remove_connection(int fd) {
    epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);

    std::unique_ptr<ConnectionContext> ctx;
    {
        std::lock_guard<std::mutex> lk(connections_mutex_);
        auto it = connections_.find(fd);
        if (it != connections_.end()) {
            ctx = std::move(it->second);
            connections_.erase(it);
        }
    }

    if (ctx && db_manager_ && ctx->tx_manager) {
        ctx->tx_manager->abort_all(db_manager_->get_database().get());
    }

    close(fd);
    LOG_INFO("ThreadGroup %d: removed connection fd=%d", group_id_, fd);
}
