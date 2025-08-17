#pragma once

#include <unordered_map>
#include <atomic>
#include <cstdint>

#include "lineairdb/lineairdb.h"

class TransactionManager {
public:
    TransactionManager();
    ~TransactionManager() = default;
    
    int64_t generate_tx_id();
    void store_transaction(int64_t tx_id, LineairDB::Transaction* tx);
    LineairDB::Transaction* get_transaction(int64_t tx_id);
    void remove_transaction(int64_t tx_id);
    
private:
    std::unordered_map<int64_t, LineairDB::Transaction*> transactions_;
    std::atomic<int64_t> next_tx_id_;
};
