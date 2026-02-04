#pragma once

#include <string>
#include <memory>

#include "../protocol/message.hh"
#include "../storage/database_manager.hh"
#include "../storage/transaction_manager.hh"
#include "lineairdb.pb.h"

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
    void handleTxRead(const std::string& message, std::string& result);
    void handleTxWrite(const std::string& message, std::string& result);
    void handleTxScan(const std::string& message, std::string& result);
    void handleDbFence(const std::string& message, std::string& result);
    void handleDbEndTransaction(const std::string& message, std::string& result);
    void handleTxBatchOperations(const std::string& message, std::string& result);

    // utility
    bool key_prefix_is_matching(const std::string& key_prefix, const std::string& key);

    // internal helpers for batch processing
    void processReadOperation(int64_t tx_id, const std::string& key,
                              LineairDB::Protocol::TxRead::Response* response);
    void processWriteOperation(int64_t tx_id, const std::string& key, const std::string& value,
                               LineairDB::Protocol::TxWrite::Response* response);
    void processScanOperation(int64_t tx_id, const std::string& db_table_key,
                              const std::string& first_key_part,
                              LineairDB::Protocol::TxScan::Response* response);
};
