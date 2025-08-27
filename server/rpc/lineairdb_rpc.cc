#include "lineairdb_rpc.hh"
#include "../../common/log.h"

#include <iostream>
#include <vector>

#include "lineairdb.pb.h"

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
    
    // Start new transaction
    auto& tx = db_manager_->get_database()->BeginTransaction();
    int64_t tx_id = tx_manager_->generate_tx_id();
    tx_manager_->store_transaction(tx_id, &tx);
    
    response.set_transaction_id(tx_id);
    result = response.SerializeAsString();
    
    LOG_DEBUG("Created transaction: %ld", tx_id);
}

void LineairDBRpc::handleTxAbort(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling TxAbort");
    
    LineairDB::Protocol::TxAbort::Request request;
    LineairDB::Protocol::TxAbort::Response response;
    
    request.ParseFromString(message);
    
    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    if (tx) {
        tx->Abort();
        LOG_DEBUG("Aborted transaction: %ld", tx_id);
    } else {
        LOG_WARNING("Transaction not found for abort: %ld", tx_id);
    }
    
    result = response.SerializeAsString();
}

void LineairDBRpc::handleTxIsAborted(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling TxIsAborted");
    
    LineairDB::Protocol::TxIsAborted::Request request;
    LineairDB::Protocol::TxIsAborted::Response response;
    
    LOG_DEBUG("Parsing request from string of size: %zu", message.size());
    request.ParseFromString(message);
    
    int64_t tx_id = request.transaction_id();
    LOG_DEBUG("Extracted transaction_id: %ld", tx_id);
    
    auto* tx = tx_manager_->get_transaction(tx_id);
    if (tx) {
        bool is_aborted = tx->IsAborted();
        response.set_is_aborted(is_aborted);
        LOG_DEBUG("Transaction %ld aborted status: %s", tx_id, is_aborted ? "true" : "false");
    } else {
        response.set_is_aborted(true);  // not found = aborted
        LOG_WARNING("Transaction not found, considering as aborted: %ld", tx_id);
    }
    
    result = response.SerializeAsString();
    LOG_DEBUG("Serialized response, size: %zu", result.size());
    LOG_DEBUG("Response is_aborted value: %s", response.is_aborted() ? "true" : "false");
}

void LineairDBRpc::handleTxRead(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling TxRead");
    
    LineairDB::Protocol::TxRead::Request request;
    LineairDB::Protocol::TxRead::Response response;
    
    request.ParseFromString(message);
    
    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    if (tx) {
        response.set_is_aborted(tx->IsAborted());

        auto read_result = tx->Read(request.key());
        if (read_result.first != nullptr) {
            response.set_found(true);
            std::string value(reinterpret_cast<const char*>(read_result.first), read_result.second);
            response.set_value(value);
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
}

void LineairDBRpc::handleTxWrite(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling TxWrite");
    
    LineairDB::Protocol::TxWrite::Request request;
    LineairDB::Protocol::TxWrite::Response response;
    
    request.ParseFromString(message);
    
    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    if (tx) {
        response.set_is_aborted(tx->IsAborted());

        const std::string& value_str = request.value();
        tx->Write(request.key(), reinterpret_cast<const std::byte*>(value_str.c_str()), value_str.size());
        response.set_success(true);
        LOG_DEBUG("Wrote key '%s' to transaction %ld", request.key().c_str(), tx_id);
    } else {
        response.set_success(false);
        response.set_is_aborted(true);  // not found assumes aborted
        LOG_WARNING("Transaction not found for write: %ld", tx_id);
    }
    
    result = response.SerializeAsString();
}

void LineairDBRpc::handleTxScan(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling TxScan");
    
    LineairDB::Protocol::TxScan::Request request;
    LineairDB::Protocol::TxScan::Response response;
    
    request.ParseFromString(message);
    
    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    if (tx) {
        response.set_is_aborted(tx->IsAborted());

        std::string table_prefix = request.db_table_key();
        std::string key_prefix = table_prefix + request.first_key_part();
        std::string scan_end_key = key_prefix + "\xFF";  // End of prefix range
        
        LOG_DEBUG("SCAN: tx_id=%ld, table_prefix='%s', first_key_part='%s', key_prefix='%s'", tx_id, table_prefix.c_str(), request.first_key_part().c_str(), key_prefix.c_str());

        tx->Scan(key_prefix, scan_end_key, [&response, &key_prefix, &table_prefix, this](const std::string_view key, const std::pair<const void*, const size_t>& value) {
            std::string key_str(key);
            LOG_DEBUG("SCAN CALLBACK: processing key='%s' with prefix='%s'", key_str.c_str(), key_prefix.c_str());

            if (key_prefix_is_matching(key_prefix, key_str)) {
                if (value.first != nullptr) {
                    std::string relative_key = key_str.substr(table_prefix.size());
                    LOG_DEBUG("SCAN CALLBACK: key matches, adding relative_key='%s'", relative_key.c_str());
                    
                    // Create key-value pair
                    auto* kv = response.add_key_values();
                    kv->set_key(relative_key);
                    kv->set_value(reinterpret_cast<const char*>(value.first), value.second);
                }
            } else {
                LOG_DEBUG("SCAN CALLBACK: unexpected key outside prefix range, skipping");
            }
            return false;
        });

        LOG_DEBUG("SCAN: completed scan, found %d key-value pairs", response.key_values_size());
    } else {
        response.set_is_aborted(true);  // not found assumes aborted
        LOG_WARNING("Transaction not found for scan: %ld", tx_id);
    }
    
    result = response.SerializeAsString();
}

void LineairDBRpc::handleDbFence(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling DbFence");
    
    LineairDB::Protocol::DbFence::Request request;
    LineairDB::Protocol::DbFence::Response response;
    
    request.ParseFromString(message);
    
    db_manager_->get_database()->Fence();
    LOG_DEBUG("Database fence completed");
    
    result = response.SerializeAsString();
}

void LineairDBRpc::handleDbEndTransaction(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling DbEndTransaction");
    
    LineairDB::Protocol::DbEndTransaction::Request request;
    LineairDB::Protocol::DbEndTransaction::Response response;
    
    request.ParseFromString(message);
    
    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    if (tx) {
        bool fence = request.fence();
        response.set_is_aborted(tx->IsAborted());
        db_manager_->get_database()->EndTransaction(*tx, [fence, tx_id](LineairDB::TxStatus status) {
            LOG_DEBUG("Transaction %ld ended with status: %d, fence=%s", tx_id, static_cast<int>(status), fence ? "true" : "false");
        });
        tx_manager_->remove_transaction(tx_id);
        LOG_DEBUG("Ended transaction %ld with fence=%s", tx_id, fence ? "true" : "false");
    } else {
        response.set_is_aborted(true);  // not found assumes aborted
        LOG_WARNING("Transaction not found for end: %ld", tx_id);
    }
    
    result = response.SerializeAsString();
}
