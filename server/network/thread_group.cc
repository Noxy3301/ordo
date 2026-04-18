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
// Idle timeout for epoll_wait. Surplus workers self-retire when this fires;
// the baseline worker never does (try_retire_local keeps >=1 alive). Value
// ported from Percona Server's thread_pool_idle_timeout default (60s):
// https://github.com/percona/percona-server/blob/8.0/sql/sys_vars.cc#L5436-L5441
static constexpr int EPOLL_TIMEOUT_MS = 60000;
static constexpr int SOCKET_TIMEOUT_SEC = 30;

// Per-group upper bound on worker count. Total workers are also capped
// process-wide by ThreadPoolCoordinator.
static constexpr uint32_t MAX_WORKERS_PER_GROUP = 16;

// Spawn throttle: the more workers we have, the longer we wait before
// spawning another. Ported from Percona Thread Pool's
// microsecond_throttling_interval() at:
// https://github.com/percona/percona-server/blob/8.0/sql/threadpool_unix.cc#L834-L845
static std::chrono::milliseconds throttle_interval(uint32_t worker_count) {
    if (worker_count < 4)  return std::chrono::milliseconds(0);
    if (worker_count < 8)  return std::chrono::milliseconds(50);
    if (worker_count < 16) return std::chrono::milliseconds(100);
    return std::chrono::milliseconds(200);
}

ThreadGroup::ThreadGroup(int group_id, ThreadPoolCoordinator* coordinator)
    : group_id_(group_id), coordinator_(coordinator) {}

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

    // eventfd: written by shutdown() to wake workers out of epoll_wait
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

    // Spawn the initial worker under workers_mutex_ (spawn_worker_unlocked
    // assumes the caller already holds it).
    bool spawned = false;
    {
        std::lock_guard<std::mutex> lk(workers_mutex_);
        spawned = spawn_worker_unlocked();
    }
    if (!spawned) {
        LOG_ERROR("ThreadGroup %d: failed to spawn initial worker", group_id_);
        close(shutdown_event_fd_); shutdown_event_fd_ = -1;
        close(epoll_fd_); epoll_fd_ = -1;
        return false;
    }
    return true;
}

void ThreadGroup::shutdown() {
    shutdown_.store(true, std::memory_order_release);

    // Wake any worker sleeping in epoll_wait. Workers blocked in recv()
    // wait for SO_RCVTIMEO (30s) or a peer close — eventfd cannot unblock recv.
    if (shutdown_event_fd_ >= 0) {
        uint64_t v = 1;
        ssize_t r = write(shutdown_event_fd_, &v, sizeof(v));
        (void)r;  // silence -Wunused-result
    }

    // Join all workers (including those already retired, which are already exited).
    {
        std::lock_guard<std::mutex> lk(workers_mutex_);
        for (auto& entry : workers_) {
            if (entry->thread.joinable()) {
                entry->thread.join();
            }
        }
        workers_.clear();
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

// Create a new worker thread and add it to workers_. Caller holds
// workers_mutex_. Allocation failure propagates out and crashes the
// process; for a DB server that is acceptable degradation.
bool ThreadGroup::spawn_worker_unlocked() {
    // Join and erase any workers that exited since the last spawn
    cleanup_retired_unlocked();

    if (!coordinator_->try_reserve_worker()) return false;

    // Increment worker_count_ BEFORE spawning, so a child that exits
    // immediately (e.g. shutdown already true) does not underflow the
    // counter via its tail fetch_sub(1).
    worker_count_.fetch_add(1, std::memory_order_relaxed);

    auto entry = std::make_unique<WorkerEntry>();
    WorkerEntry* self = entry.get();
    workers_.push_back(std::move(entry));
    workers_.back()->thread = std::thread(&ThreadGroup::worker_main, this, self);

    last_spawn_time_ = std::chrono::steady_clock::now();
    return true;
}

// Join and erase worker entries that have already exited. Caller holds
// workers_mutex_.
void ThreadGroup::cleanup_retired_unlocked() {
    auto it = workers_.begin();
    while (it != workers_.end()) {
        if ((*it)->retired.load(std::memory_order_acquire)) {
            if ((*it)->thread.joinable()) {
                (*it)->thread.join();
            }
            it = workers_.erase(it);
        } else {
            ++it;
        }
    }
}

void ThreadGroup::maybe_spawn_worker() {
    std::lock_guard<std::mutex> lk(workers_mutex_);
    // Join and erase any workers that exited since the last check
    cleanup_retired_unlocked();

    uint32_t cur = worker_count_.load(std::memory_order_relaxed);
    if (cur >= MAX_WORKERS_PER_GROUP) return;

    auto now = std::chrono::steady_clock::now();
    if (now - last_spawn_time_ < throttle_interval(cur)) return;

    spawn_worker_unlocked();
}

// Decide whether the calling worker should exit. CAS-decrements
// worker_count_ only when >1 worker exists so the baseline worker is
// always preserved. On shutdown, short-circuits true so the worker exits
// (retired stays false, tail decrements worker_count_).
bool ThreadGroup::try_retire_local(bool* retired) {
    if (shutdown_.load(std::memory_order_relaxed)) return true;

    uint32_t cur = worker_count_.load(std::memory_order_relaxed);
    while (cur > 1) {
        if (worker_count_.compare_exchange_weak(
                cur, cur - 1,
                std::memory_order_acq_rel, std::memory_order_relaxed)) {
            *retired = true;
            return true;
        }
    }
    return false;
}

void ThreadGroup::worker_main(WorkerEntry* self) {
    LOG_INFO("ThreadGroup %d: worker started", group_id_);
    bool retired = false;

    struct epoll_event events[MAX_EVENTS];

    while (!shutdown_.load(std::memory_order_relaxed)) {
        int n = epoll_wait(epoll_fd_, events, MAX_EVENTS, EPOLL_TIMEOUT_MS);
        if (n < 0) {
            if (errno == EINTR) continue;
            LOG_ERROR("ThreadGroup %d: epoll_wait failed: %s",
                      group_id_, std::strerror(errno));
            break;
        }
        if (n == 0) {
            // Idle timeout. Surplus workers retire here; baseline stays.
            if (try_retire_local(&retired)) break;
            continue;
        }

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

    // Exit paths:
    //   retired==true : try_retire_local already decremented worker_count_
    //   otherwise     : decrement here (shutdown or epoll_wait error)
    coordinator_->release_worker();
    if (!retired) worker_count_.fetch_sub(1, std::memory_order_relaxed);

    // Mark the entry reapable. spawn_worker_unlocked / shutdown() will join it.
    if (self) self->retired.store(true, std::memory_order_release);
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
