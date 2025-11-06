#include "lineairdb_rpc.hh"
#include "../../common/log.h"

#include <chrono>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <mutex>
#include <vector>

#include "lineairdb.pb.h"

namespace {

const std::string& GetTimingLogPath() {
    static const std::string path = []() {
        const char* env = std::getenv("LINEAIRDB_PROTOBUF_TIMING_LOG");
        if (env && env[0] != '\0') {
            return std::string(env);
        }
        return std::string("/home/noxy/ordo/lineairdb_logs/protobuf_timing.log");
    }();
    return path;
}

const char* MessageTypeToString(MessageType type) {
    switch (type) {
        case MessageType::TX_BEGIN_TRANSACTION: return "TX_BEGIN_TRANSACTION";
        case MessageType::TX_ABORT: return "TX_ABORT";
        case MessageType::TX_READ: return "TX_READ";
        case MessageType::TX_WRITE: return "TX_WRITE";
        case MessageType::TX_SCAN: return "TX_SCAN";
        case MessageType::DB_FENCE: return "DB_FENCE";
        case MessageType::DB_END_TRANSACTION: return "DB_END_TRANSACTION";
        default: return "UNKNOWN";
    }
}

void AppendServerTiming(MessageType type,
                        std::chrono::steady_clock::time_point total_start,
                        std::chrono::steady_clock::time_point total_end,
                        std::chrono::steady_clock::time_point exec_start,
                        std::chrono::steady_clock::time_point exec_end,
                        size_t request_bytes,
                        size_t response_bytes) {
    const std::string& path = GetTimingLogPath();
    if (path.empty()) return;

    static std::mutex file_mutex;
    std::lock_guard<std::mutex> lock(file_mutex);

    std::ofstream out(path, std::ios::app);
    if (!out) return;

    auto to_ns = [](const std::chrono::steady_clock::time_point& tp) {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(
                   tp.time_since_epoch())
            .count();
    };

    auto clamp_exec = [](std::chrono::steady_clock::time_point start,
                         std::chrono::steady_clock::time_point end) {
        if (end < start) return std::chrono::steady_clock::time_point{start};
        return end;
    };

    exec_end = clamp_exec(exec_start, exec_end);
    total_end = clamp_exec(total_start, total_end);

    const long long exec_ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(exec_end - exec_start)
            .count();
    const long long total_ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(total_end - total_start)
            .count();

    out << "message=" << MessageTypeToString(type)
        << " serialize_start_ns=" << to_ns(total_start)
        << " serialize_end_ns=" << to_ns(total_start)
        << " deserialize_start_ns=" << to_ns(total_end)
        << " deserialize_end_ns=" << to_ns(total_end)
        << " serialize_ns=0"
        << " deserialize_ns=0"
        << " send_ns=0"
        << " recv_ns=0"
        << " roundtrip_ns=" << total_ns
        << " lineairdb_exec_ns=" << exec_ns
        << " request_bytes=" << request_bytes
        << " response_bytes=" << response_bytes
        << " source=server"
        << " parse_ok=1"
        << std::endl;
}

}  // namespace

LineairDBRpc::LineairDBRpc(std::shared_ptr<DatabaseManager> db_manager,
                           std::shared_ptr<TransactionManager> tx_manager) 
    : db_manager_(db_manager), tx_manager_(tx_manager) {
}

void LineairDBRpc::handle_rpc(uint64_t sender_id, MessageType message_type, 
                             const std::string& message, std::string& result) {
    LOG_DEBUG("Handling RPC: message_type=%u", static_cast<uint32_t>(message_type));
    
    switch(message_type) {
        case MessageType::TX_BEGIN_TRANSACTION:
            handleTxBeginTransaction(message, result);
            return;
        case MessageType::TX_ABORT:
            handleTxAbort(message, result);
            return;
        case MessageType::TX_READ:
            handleTxRead(message, result);
            return;
        case MessageType::TX_WRITE:
            handleTxWrite(message, result);
            return;
        case MessageType::TX_SCAN:
            handleTxScan(message, result);
            return;
        case MessageType::DB_FENCE:
            handleDbFence(message, result);
            return;
        case MessageType::DB_END_TRANSACTION:
            handleDbEndTransaction(message, result);
            return;
        default:
            LOG_ERROR("Unknown message type: %u", static_cast<uint32_t>(message_type));
            return;
    }
}

bool LineairDBRpc::key_prefix_is_matching(const std::string& key_prefix, const std::string& key) {
    if (key.substr(0, key_prefix.size()) != key_prefix) return false;
    return true;
}

void LineairDBRpc::handleTxBeginTransaction(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling TxBeginTransaction");
    
    LineairDB::Protocol::TxBeginTransaction::Request request;
    LineairDB::Protocol::TxBeginTransaction::Response response;
    
    request.ParseFromString(message);
    
    auto total_start = std::chrono::steady_clock::now();
    auto exec_start = total_start;
    auto& tx = db_manager_->get_database()->BeginTransaction();
    auto exec_end = std::chrono::steady_clock::now();
    int64_t tx_id = tx_manager_->generate_tx_id();
    tx_manager_->store_transaction(tx_id, &tx);
    
    response.set_transaction_id(tx_id);
    result = response.SerializeAsString();
    
    LOG_DEBUG("Created transaction: %ld", tx_id);

    auto total_end = std::chrono::steady_clock::now();
    AppendServerTiming(MessageType::TX_BEGIN_TRANSACTION, total_start, total_end,
                       exec_start, exec_end, 0, 0);
}

void LineairDBRpc::handleTxAbort(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling TxAbort");
    
    LineairDB::Protocol::TxAbort::Request request;
    LineairDB::Protocol::TxAbort::Response response;
    
    request.ParseFromString(message);
    
    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    auto total_start = std::chrono::steady_clock::now();
    auto exec_start = total_start;
    auto exec_end = exec_start;
    if (tx) {
        exec_start = std::chrono::steady_clock::now();
        tx->Abort();
        exec_end = std::chrono::steady_clock::now();
    } else {
        LOG_WARNING("Transaction not found for abort: %ld", tx_id);
    }
    
    result = response.SerializeAsString();

    auto total_end = std::chrono::steady_clock::now();
    AppendServerTiming(MessageType::TX_ABORT, total_start, total_end, exec_start,
                       exec_end, 0, 0);
}

void LineairDBRpc::handleTxRead(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling TxRead");
    
    LineairDB::Protocol::TxRead::Request request;
    LineairDB::Protocol::TxRead::Response response;
    
    request.ParseFromString(message);
    
    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    auto total_start = std::chrono::steady_clock::now();
    auto exec_start = total_start;
    auto exec_end = exec_start;
    size_t response_bytes = 0;
    if (tx) {
        response.set_is_aborted(tx->IsAborted());

        exec_start = std::chrono::steady_clock::now();
        auto read_result = tx->Read(request.key());
        exec_end = std::chrono::steady_clock::now();
        if (read_result.first != nullptr) {
            response.set_found(true);
            std::string value(reinterpret_cast<const char*>(read_result.first), read_result.second);
            response.set_value(value);
            response_bytes = value.size();
        } else {
            response.set_found(false);
        }
        LOG_DEBUG("Read key '%s' from transaction %ld: %s", request.key().c_str(), tx_id, (read_result.first != nullptr ? "found" : "not found"));
    } else {
        response.set_found(false);
        response.set_is_aborted(true);  // not found assumes aborted
        LOG_WARNING("Transaction not found for read: %ld", tx_id);
    }
    
    result = response.SerializeAsString();

    auto total_end = std::chrono::steady_clock::now();
    AppendServerTiming(MessageType::TX_READ, total_start, total_end, exec_start,
                       exec_end, request.key().size(), response_bytes);
}

void LineairDBRpc::handleTxWrite(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling TxWrite");
    
    LineairDB::Protocol::TxWrite::Request request;
    LineairDB::Protocol::TxWrite::Response response;
    
    request.ParseFromString(message);
    
    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    auto total_start = std::chrono::steady_clock::now();
    auto exec_start = total_start;
    auto exec_end = exec_start;
    if (tx) {
        response.set_is_aborted(tx->IsAborted());

        const std::string& value_str = request.value();
        exec_start = std::chrono::steady_clock::now();
        tx->Write(request.key(), reinterpret_cast<const std::byte*>(value_str.c_str()), value_str.size());
        exec_end = std::chrono::steady_clock::now();
        response.set_success(true);
        LOG_DEBUG("Wrote key '%s' to transaction %ld", request.key().c_str(), tx_id);
    } else {
        response.set_success(false);
        response.set_is_aborted(true);  // not found assumes aborted
        LOG_WARNING("Transaction not found for write: %ld", tx_id);
    }
    
    result = response.SerializeAsString();

    auto total_end = std::chrono::steady_clock::now();
    AppendServerTiming(MessageType::TX_WRITE, total_start, total_end, exec_start,
                       exec_end, request.key().size() + request.value().size(), 0);
}

void LineairDBRpc::handleTxScan(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling TxScan");
    
    LineairDB::Protocol::TxScan::Request request;
    LineairDB::Protocol::TxScan::Response response;
    
    request.ParseFromString(message);
    
    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    auto total_start = std::chrono::steady_clock::now();
    auto exec_start = total_start;
    auto exec_end = exec_start;
    size_t response_bytes = 0;
    if (tx) {
        response.set_is_aborted(tx->IsAborted());

        std::string table_prefix = request.db_table_key();
        std::string key_prefix = table_prefix + request.first_key_part();
        std::string scan_end_key = key_prefix + "\xFF";  // End of prefix range
        
        LOG_DEBUG("SCAN: tx_id=%ld, table_prefix='%s', first_key_part='%s', key_prefix='%s'", tx_id, table_prefix.c_str(), request.first_key_part().c_str(), key_prefix.c_str());

        exec_start = std::chrono::steady_clock::now();
        tx->Scan(key_prefix, scan_end_key,
                 [&response, &key_prefix, &table_prefix, &response_bytes, this](const std::string_view key,
                                                                                const std::pair<const void*, const size_t>& value) {
                     std::string key_str(key);
                     LOG_DEBUG("SCAN CALLBACK: processing key='%s' with prefix='%s'", key_str.c_str(), key_prefix.c_str());

                     if (key_prefix_is_matching(key_prefix, key_str)) {
                         if (value.first != nullptr) {
                             std::string relative_key = key_str.substr(table_prefix.size());
                             LOG_DEBUG("SCAN CALLBACK: key matches, adding relative_key='%s'", relative_key.c_str());
                             
                             auto* kv = response.add_key_values();
                             kv->set_key(relative_key);
                             kv->set_value(reinterpret_cast<const char*>(value.first), value.second);
                             response_bytes += relative_key.size() + value.second;
                         }
                     } else {
                         LOG_DEBUG("SCAN CALLBACK: unexpected key outside prefix range, skipping");
                     }
                     return false;
                 });
        exec_end = std::chrono::steady_clock::now();

        LOG_DEBUG("SCAN: completed scan, found %d key-value pairs", response.key_values_size());
    } else {
        response.set_is_aborted(true);  // not found assumes aborted
        LOG_WARNING("Transaction not found for scan: %ld", tx_id);
    }
    
    result = response.SerializeAsString();

    auto total_end = std::chrono::steady_clock::now();
    AppendServerTiming(MessageType::TX_SCAN, total_start, total_end, exec_start,
                       exec_end, request.db_table_key().size() + request.first_key_part().size(),
                       response_bytes);
}

void LineairDBRpc::handleDbFence(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling DbFence");
    
    LineairDB::Protocol::DbFence::Request request;
    LineairDB::Protocol::DbFence::Response response;
    
    request.ParseFromString(message);
    
    auto total_start = std::chrono::steady_clock::now();
    auto exec_start = total_start;
    db_manager_->get_database()->Fence();
    auto exec_end = std::chrono::steady_clock::now();
    LOG_DEBUG("Database fence completed");
    
    result = response.SerializeAsString();

    auto total_end = std::chrono::steady_clock::now();
    AppendServerTiming(MessageType::DB_FENCE, total_start, total_end, exec_start,
                       exec_end, 0, 0);
}

void LineairDBRpc::handleDbEndTransaction(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling DbEndTransaction");
    
    LineairDB::Protocol::DbEndTransaction::Request request;
    LineairDB::Protocol::DbEndTransaction::Response response;
    
    request.ParseFromString(message);
    
    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    auto total_start = std::chrono::steady_clock::now();
    auto exec_start = total_start;
    auto exec_end = exec_start;
    if (tx) {
        bool fence = request.fence();
        exec_start = std::chrono::steady_clock::now();
        bool committed = db_manager_->get_database()->EndTransaction(
            *tx, [fence, tx_id](LineairDB::TxStatus status) {
                LOG_DEBUG("Transaction %ld ended with status: %d, fence=%s", tx_id, static_cast<int>(status), fence ? "true" : "false");
            });
        exec_end = std::chrono::steady_clock::now();
        bool aborted = !committed;
        response.set_is_aborted(aborted);
        tx_manager_->remove_transaction(tx_id);
        LOG_DEBUG("Ended transaction %ld with fence=%s (committed=%s)", tx_id, fence ? "true" : "false", committed ? "true" : "false");
    } else {
        response.set_is_aborted(true);  // not found assumes aborted
        LOG_WARNING("Transaction not found for end: %ld", tx_id);
    }
    
    result = response.SerializeAsString();

    auto total_end = std::chrono::steady_clock::now();
    AppendServerTiming(MessageType::DB_END_TRANSACTION, total_start, total_end,
                       exec_start, exec_end, 0, 0);
}
