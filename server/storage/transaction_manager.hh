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

    bool has_active_transactions() const { return !transactions_.empty(); }

    // Abort and end all in-flight transactions (for disconnect cleanup).
    void abort_all(LineairDB::Database* db);

private:
    std::unordered_map<int64_t, LineairDB::Transaction*> transactions_;
    std::atomic<int64_t> next_tx_id_;
};
