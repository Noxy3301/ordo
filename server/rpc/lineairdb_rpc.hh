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

    // Transaction lifecycle
    void handleTxBeginTransaction(const std::string& message, std::string& result);
    void handleTxAbort(const std::string& message, std::string& result);

    // Primary key operations
    void handleTxRead(const std::string& message, std::string& result);
    void handleTxWrite(const std::string& message, std::string& result);
    void handleTxDelete(const std::string& message, std::string& result);

    // Secondary index operations
    void handleTxReadSecondaryIndex(const std::string& message, std::string& result);
    void handleTxWriteSecondaryIndex(const std::string& message, std::string& result);
    void handleTxDeleteSecondaryIndex(const std::string& message, std::string& result);
    void handleTxUpdateSecondaryIndex(const std::string& message, std::string& result);

    // Primary key scan operations
    void handleTxGetMatchingKeysInRange(const std::string& message, std::string& result);
    void handleTxGetMatchingKeysAndValuesInRange(const std::string& message, std::string& result);
    void handleTxGetMatchingKeysAndValuesFromPrefix(const std::string& message, std::string& result);
    void handleTxFetchLastKeyInRange(const std::string& message, std::string& result);
    void handleTxFetchFirstKeyWithPrefix(const std::string& message, std::string& result);
    void handleTxFetchNextKeyWithPrefix(const std::string& message, std::string& result);

    // Secondary index scan operations
    void handleTxGetMatchingPrimaryKeysInRange(const std::string& message, std::string& result);
    void handleTxGetMatchingPrimaryKeysFromPrefix(const std::string& message, std::string& result);
    void handleTxFetchLastPrimaryKeyInSecondaryRange(const std::string& message, std::string& result);
    void handleTxFetchLastSecondaryEntryInRange(const std::string& message, std::string& result);

    // Database operations
    void handleDbFence(const std::string& message, std::string& result);
    void handleDbEndTransaction(const std::string& message, std::string& result);
    void handleDbCreateTable(const std::string& message, std::string& result);
    void handleDbSetTable(const std::string& message, std::string& result);
    void handleDbCreateSecondaryIndex(const std::string& message, std::string& result);

    // utility
    bool key_prefix_is_matching(const std::string& key_prefix, const std::string& key);
};
