#include "transaction_manager.hh"
#include "../../common/log.h"

#include <iostream>

TransactionManager::TransactionManager() : next_tx_id_(1) {}

int64_t TransactionManager::generate_tx_id() {
    return next_tx_id_.fetch_add(1);
}

void TransactionManager::store_transaction(int64_t tx_id, LineairDB::Transaction* tx) {
    transactions_[tx_id] = tx;
}

LineairDB::Transaction* TransactionManager::get_transaction(int64_t tx_id) {
    auto it = transactions_.find(tx_id);
    if (it == transactions_.end()) {
        LOG_WARNING("Transaction not found: %ld", tx_id);
        return nullptr;
    }
    return it->second;
}

void TransactionManager::remove_transaction(int64_t tx_id) {
    transactions_.erase(tx_id);
}

void TransactionManager::abort_all(LineairDB::Database* db) {
    for (auto& [tx_id, tx] : transactions_) {
        if (tx) {
            tx->Abort();
            // EndTransaction requires the calling thread to be online
            // (it calls MakeMeOffline internally).
            db->EnsureThreadOnline();
            db->EndTransaction(*tx, [](LineairDB::TxStatus) {});
        }
    }
    transactions_.clear();
}
