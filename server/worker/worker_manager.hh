#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>

#include "transaction_worker.hh"

// Manages TransactionWorker instances per connection.
class WorkerManager {
public:
    explicit WorkerManager(std::shared_ptr<DatabaseManager> db_manager,
                           std::shared_ptr<TransactionManager> tx_manager);

    // Create a new worker and run BeginTransaction through it.
    // Returns serialized response. Also returns tx_id via out param when possible.
    std::string begin_on_new_worker(uint64_t sender_id,
                                    const std::string& payload,
                                    int64_t& out_tx_id);

    // Enqueue a message to an existing worker by tx_id. Returns empty string if missing.
    std::string dispatch_to_worker(uint64_t sender_id,
                                   int64_t tx_id,
                                   MessageType type,
                                   const std::string& payload);

    // Remove and shutdown worker for tx_id (called after EndTransaction is processed).
    void remove_worker(int64_t tx_id);

private:
    std::shared_ptr<DatabaseManager> db_manager_;
    std::shared_ptr<TransactionManager> tx_manager_;
    std::unordered_map<int64_t, std::unique_ptr<TransactionWorker>> workers_;
};

