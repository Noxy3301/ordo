#pragma once

#include <string>
#include <memory>

#include "../protocol/message.hh"
#include "../storage/database_manager.hh"
#include "../storage/transaction_manager.hh"

class LineairDBRpc {
public:
    LineairDBRpc(std::shared_ptr<DatabaseManager> db_manager,
                 std::shared_ptr<TransactionManager> tx_manager);
    ~LineairDBRpc() = default;
    
    void handle_rpc(uint64_t sender_id, MessageType message_type, 
                   const std::string& message, std::string& result);
    
private:
    std::shared_ptr<DatabaseManager> db_manager_;
    std::shared_ptr<TransactionManager> tx_manager_;
    
    // RPC handlers
    void handleTxBeginTransaction(const std::string& message, std::string& result);
    void handleTxAbort(const std::string& message, std::string& result);
    void handleTxIsAborted(const std::string& message, std::string& result);
    void handleTxRead(const std::string& message, std::string& result);
    void handleTxWrite(const std::string& message, std::string& result);
    void handleTxScan(const std::string& message, std::string& result);
    void handleDbFence(const std::string& message, std::string& result);
    void handleDbEndTransaction(const std::string& message, std::string& result);
    
    // utility
    bool key_prefix_is_matching(const std::string& key_prefix, const std::string& key);
};
