#pragma once

#include <iostream>
#include <unordered_map>
#include <memory>
#include <atomic>
#include <thread>
#include <cstdint>

#include "lineairdb/lineairdb.h"
#include "lineairdb.pb.h"

// Message header for RPC communication
struct MessageHeader {
    uint64_t sender_id;      // sender ID (not used in LineairDB but keeping for consistency)
    uint32_t message_type;   // OpCode from protobuf
    uint32_t payload_size;   // size of the protobuf payload
};

// MessageType enum (corresponds to protobuf OpCode)
enum class MessageType : uint32_t {
    UNKNOWN = 0,
    TX_BEGIN_TRANSACTION = 1,
    TX_ABORT = 2,
    TX_IS_ABORTED = 3,
    TX_READ = 4,
    TX_WRITE = 5,
    TX_SCAN = 6,
    DB_FENCE = 7,
    DB_END_TRANSACTION = 8
};

class LineairDBServer {
public:
    LineairDBServer();
    ~LineairDBServer() = default;

    void run();

private:
    std::shared_ptr<LineairDB::Database> database_;
    std::unordered_map<int64_t, LineairDB::Transaction*> transactions_;
    std::atomic<int64_t> next_tx_id_;

    inline int64_t generate_tx_id() { return next_tx_id_.fetch_add(1); }
    LineairDB::Transaction* get_transaction(int64_t tx_id);
    bool key_prefix_is_matching(const std::string& key_prefix, const std::string& key);
    void handle_client(int client_socket);
    void handleRPC(uint64_t sender_id, MessageType message_type, const std::string& message, std::string& result);
    
    // RPC handlers (Raft pattern)
    void handleTxBeginTransaction(const std::string& message, std::string& result);
    void handleTxAbort(const std::string& message, std::string& result);
    void handleTxIsAborted(const std::string& message, std::string& result);
    void handleTxRead(const std::string& message, std::string& result);
    void handleTxWrite(const std::string& message, std::string& result);
    void handleTxScan(const std::string& message, std::string& result);
    void handleDbFence(const std::string& message, std::string& result);
    void handleDbEndTransaction(const std::string& message, std::string& result);
};