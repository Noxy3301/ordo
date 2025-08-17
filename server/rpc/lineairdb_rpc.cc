#include "lineairdb_rpc.hh"

#include <iostream>
#include <vector>

#include "lineairdb.pb.h"

LineairDBRpc::LineairDBRpc(std::shared_ptr<DatabaseManager> db_manager,
                           std::shared_ptr<TransactionManager> tx_manager) 
    : db_manager_(db_manager), tx_manager_(tx_manager) {
}

void LineairDBRpc::handle_rpc(uint64_t sender_id, MessageType message_type, 
                             const std::string& message, std::string& result) {
    std::cout << "Handling RPC: message_type=" << static_cast<uint32_t>(message_type) << std::endl;
    
    switch(message_type) {
        case MessageType::TX_BEGIN_TRANSACTION:
            handleTxBeginTransaction(message, result);
            return;
        case MessageType::TX_ABORT:
            handleTxAbort(message, result);
            return;
        case MessageType::TX_IS_ABORTED:
            handleTxIsAborted(message, result);
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
            std::cout << "Unknown message type: " << static_cast<uint32_t>(message_type) << std::endl;
            return;
    }
}

bool LineairDBRpc::key_prefix_is_matching(const std::string& key_prefix, const std::string& key) {
    if (key.substr(0, key_prefix.size()) != key_prefix) return false;
    return true;
}

void LineairDBRpc::handleTxBeginTransaction(const std::string& message, std::string& result) {
    std::cout << "Handling TxBeginTransaction" << std::endl;
    
    LineairDB::Protocol::TxBeginTransaction::Request request;
    LineairDB::Protocol::TxBeginTransaction::Response response;
    
    request.ParseFromString(message);
    
    // Start new transaction
    auto& tx = db_manager_->get_database()->BeginTransaction();
    int64_t tx_id = tx_manager_->generate_tx_id();
    tx_manager_->store_transaction(tx_id, &tx);
    
    response.set_transaction_id(tx_id);
    result = response.SerializeAsString();
    
    std::cout << "Created transaction: " << tx_id << std::endl;
}

void LineairDBRpc::handleTxAbort(const std::string& message, std::string& result) {
    std::cout << "Handling TxAbort" << std::endl;
    
    LineairDB::Protocol::TxAbort::Request request;
    LineairDB::Protocol::TxAbort::Response response;
    
    request.ParseFromString(message);
    
    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    if (tx) {
        tx->Abort();
        std::cout << "Aborted transaction: " << tx_id << std::endl;
    } else {
        std::cout << "Transaction not found for abort: " << tx_id << std::endl;
    }
    
    result = response.SerializeAsString();
}

void LineairDBRpc::handleTxIsAborted(const std::string& message, std::string& result) {
    std::cout << "Handling TxIsAborted" << std::endl;
    
    LineairDB::Protocol::TxIsAborted::Request request;
    LineairDB::Protocol::TxIsAborted::Response response;
    
    std::cout << "DEBUG: Parsing request from string of size: " << message.size() << std::endl;
    request.ParseFromString(message);
    
    int64_t tx_id = request.transaction_id();
    std::cout << "DEBUG: Extracted transaction_id: " << tx_id << std::endl;
    
    auto* tx = tx_manager_->get_transaction(tx_id);
    if (tx) {
        bool is_aborted = tx->IsAborted();
        response.set_is_aborted(is_aborted);
        std::cout << "Transaction " << tx_id << " aborted status: " << is_aborted << std::endl;
    } else {
        response.set_is_aborted(true);  // not found = aborted
        std::cout << "Transaction not found, considering as aborted: " << tx_id << std::endl;
    }
    
    result = response.SerializeAsString();
    std::cout << "DEBUG: Serialized response, size: " << result.size() << std::endl;
    std::cout << "DEBUG: Response is_aborted value: " << response.is_aborted() << std::endl;
}

void LineairDBRpc::handleTxRead(const std::string& message, std::string& result) {
    std::cout << "Handling TxRead" << std::endl;
    
    LineairDB::Protocol::TxRead::Request request;
    LineairDB::Protocol::TxRead::Response response;
    
    request.ParseFromString(message);
    
    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    if (tx) {
        auto read_result = tx->Read(request.key());
        if (read_result.first != nullptr) {
            response.set_found(true);
            std::string value(reinterpret_cast<const char*>(read_result.first), read_result.second);
            response.set_value(value);
        } else {
            response.set_found(false);
        }
        std::cout << "Read key '" << request.key() << "' from transaction " << tx_id << ": " << (read_result.first != nullptr ? "found" : "not found") << std::endl;
    } else {
        response.set_found(false);
        std::cout << "Transaction not found for read: " << tx_id << std::endl;
    }
    
    result = response.SerializeAsString();
}

void LineairDBRpc::handleTxWrite(const std::string& message, std::string& result) {
    std::cout << "Handling TxWrite" << std::endl;
    
    LineairDB::Protocol::TxWrite::Request request;
    LineairDB::Protocol::TxWrite::Response response;
    
    request.ParseFromString(message);
    
    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    if (tx) {
        const std::string& value_str = request.value();
        tx->Write(request.key(), reinterpret_cast<const std::byte*>(value_str.c_str()), value_str.size());
        response.set_success(true);
        std::cout << "Wrote key '" << request.key() << "' to transaction " << tx_id << std::endl;
    } else {
        response.set_success(false);
        std::cout << "Transaction not found for write: " << tx_id << std::endl;
    }
    
    result = response.SerializeAsString();
}

void LineairDBRpc::handleTxScan(const std::string& message, std::string& result) {
    std::cout << "Handling TxScan" << std::endl;
    
    LineairDB::Protocol::TxScan::Request request;
    LineairDB::Protocol::TxScan::Response response;
    
    request.ParseFromString(message);
    
    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    if (tx) {
        std::vector<std::string> keys;
        std::string table_prefix = request.db_table_key();
        std::string key_prefix = table_prefix + request.first_key_part();
        
        std::cout << "DEBUG SCAN: tx_id=" << tx_id << ", table_prefix='" << table_prefix 
                  << "', first_key_part='" << request.first_key_part() << "', key_prefix='" << key_prefix << "'" << std::endl;

        tx->Scan("", std::nullopt, [&keys, &key_prefix, &table_prefix, this](const std::string_view key, const std::pair<const void*, const size_t>& value) {
            std::string key_str(key);
            std::cout << "DEBUG SCAN CALLBACK: checking key='" << key_str << "' against prefix='" << key_prefix << "'" << std::endl;

            if (key_prefix_is_matching(key_prefix, key_str)) {
                std::string relative_key = key_str.substr(table_prefix.size());
                std::cout << "DEBUG SCAN CALLBACK: key matches, adding relative_key='" << relative_key << "'" << std::endl;
                keys.push_back(relative_key);
            } else {
                std::cout << "DEBUG SCAN CALLBACK: key does not match prefix, skipping" << std::endl;
            }
            return false;
        });
        
        std::cout << "DEBUG SCAN: completed scan, found " << keys.size() << " keys:" << std::endl;
        for (size_t i = 0; i < keys.size(); i++) {
            std::cout << "  [" << i << "] " << keys[i] << std::endl;
        }
        
        for (const auto& key : keys) {
            response.add_keys(key);
        }
        std::cout << "Scanned transaction " << tx_id << ", found " << keys.size() << " keys" << std::endl;
    } else {
        std::cout << "Transaction not found for scan: " << tx_id << std::endl;
    }
    
    result = response.SerializeAsString();
}

void LineairDBRpc::handleDbFence(const std::string& message, std::string& result) {
    std::cout << "Handling DbFence" << std::endl;
    
    LineairDB::Protocol::DbFence::Request request;
    LineairDB::Protocol::DbFence::Response response;
    
    request.ParseFromString(message);
    
    db_manager_->get_database()->Fence();
    std::cout << "Database fence completed" << std::endl;
    
    result = response.SerializeAsString();
}

void LineairDBRpc::handleDbEndTransaction(const std::string& message, std::string& result) {
    std::cout << "Handling DbEndTransaction" << std::endl;
    
    LineairDB::Protocol::DbEndTransaction::Request request;
    LineairDB::Protocol::DbEndTransaction::Response response;
    
    request.ParseFromString(message);
    
    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    if (tx) {
        bool fence = request.fence();
        db_manager_->get_database()->EndTransaction(*tx, [fence, tx_id](LineairDB::TxStatus status) {
            std::cout << "Transaction " << tx_id << " ended with status: " << static_cast<int>(status) << ", fence=" << fence << std::endl;
        });
        tx_manager_->remove_transaction(tx_id);
        std::cout << "Ended transaction " << tx_id << " with fence=" << fence << std::endl;
    } else {
        std::cout << "Transaction not found for end: " << tx_id << std::endl;
    }
    
    result = response.SerializeAsString();
}
