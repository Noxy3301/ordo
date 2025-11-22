#include "ordo_server.hh"
#include "../common/log.h"
#include "lineairdb.pb.h"
#include "rpc/lineairdb_rpc.hh"

#include <iostream>
#include <atomic>
#include <chrono>
#include <fstream>
#include <cstdlib>
#include <string>
#include <thread>
#include <sstream>
#include <iomanip>
#include <ctime>

namespace {
// Lightweight, opt-in per-RPC profiler. Enable with ORDO_PROFILE=1.
struct RpcMetrics {
    void record(uint64_t recv_ns, uint64_t handle_ns, uint64_t send_ns) {
        recv_ns_.fetch_add(recv_ns, std::memory_order_relaxed);
        handle_ns_.fetch_add(handle_ns, std::memory_order_relaxed);
        send_ns_.fetch_add(send_ns, std::memory_order_relaxed);
        count_.fetch_add(1, std::memory_order_relaxed);
    }

    bool drain(uint64_t& count, uint64_t& recv_ns, uint64_t& handle_ns, uint64_t& send_ns) {
        count     = count_.exchange(0, std::memory_order_relaxed);
        recv_ns   = recv_ns_.exchange(0, std::memory_order_relaxed);
        handle_ns = handle_ns_.exchange(0, std::memory_order_relaxed);
        send_ns   = send_ns_.exchange(0, std::memory_order_relaxed);
        return count != 0;
    }

private:
    std::atomic<uint64_t> count_{0};
    std::atomic<uint64_t> recv_ns_{0};
    std::atomic<uint64_t> handle_ns_{0};
    std::atomic<uint64_t> send_ns_{0};
};

RpcMetrics g_rpc_metrics;
std::atomic<bool> g_profile_enabled{false};

std::string make_profile_path() {
    auto now = std::chrono::system_clock::now();
    std::time_t t = std::chrono::system_clock::to_time_t(now);
    std::tm tm = *std::localtime(&t);
    std::ostringstream oss;
    oss << "./lineairdb_logs/ordo_rpc_profile_"
        << std::put_time(&tm, "%Y%m%d_%H%M%S")
        << ".csv";
    return oss.str();
}

void start_rpc_profiler_thread() {
    const char* env = std::getenv("ORDO_PROFILE");
    if (!env || std::string(env) != "1") return;

    g_profile_enabled.store(true, std::memory_order_relaxed);
    std::thread([]() {
        const std::string path = make_profile_path();
        std::ofstream out(path, std::ios::app);
        if (out.tellp() == 0) {
            out << "timestamp,count,recv_us,handle_us,send_us,total_us\n";
        }

        while (true) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            uint64_t count = 0, recv_ns = 0, handle_ns = 0, send_ns = 0;
            if (!g_rpc_metrics.drain(count, recv_ns, handle_ns, send_ns)) continue;

            const double recv_us   = recv_ns / 1000.0;
            const double handle_us = handle_ns / 1000.0;
            const double send_us   = send_ns / 1000.0;
            const double total_us  = recv_us + handle_us + send_us;

            const auto now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
            out << std::put_time(std::localtime(&now), "%F %T") << ','
                << count << ','
                << recv_us << ','
                << handle_us << ','
                << send_us << ','
                << total_us << '\n';
            out.flush();
        }
    }).detach();
}
}  // namespace

OrdoServer::OrdoServer() : TcpServer(9999) {}

void OrdoServer::init() {
    // Initialize components in dependency order
    if (!db_manager_) {
        db_manager_ = std::make_shared<DatabaseManager>();
    }

    start_rpc_profiler_thread();
    LOG_INFO("Ordo server initialized successfully");
}

void OrdoServer::handle_client(int client_socket) {
    LOG_INFO("Handling client connection fd=%d", client_socket);
    // Per-connection managers
    auto tx_manager = std::make_shared<TransactionManager>();
    auto rpc_handler = std::make_shared<LineairDBRpc>(db_manager_, tx_manager);

    using namespace std::chrono;

    while (true) {
        uint64_t sender_id;
        MessageType message_type;
        std::string payload;

        const auto recv_start = steady_clock::now();
        if (!MessageHandler::receive_message(client_socket, sender_id, message_type, payload)) {
            return;  // Client disconnected or error
        }
        const auto rpc_start = steady_clock::now();

        std::string result;
        rpc_handler->handle_rpc(sender_id, message_type, payload, result);
        const auto send_start = steady_clock::now();

        if (!MessageHandler::send_response(client_socket, 0, message_type, result)) {
            return;  // Failed to send response
        }

        if (g_profile_enabled.load(std::memory_order_relaxed)) {
            const auto recv_ns   = duration_cast<nanoseconds>(rpc_start - recv_start).count();
            const auto handle_ns = duration_cast<nanoseconds>(send_start - rpc_start).count();
            const auto send_ns   = duration_cast<nanoseconds>(steady_clock::now() - send_start).count();
            g_rpc_metrics.record(recv_ns, handle_ns, send_ns);
        }
    }
}
