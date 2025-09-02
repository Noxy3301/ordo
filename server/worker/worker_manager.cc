#include "worker_manager.hh"
#include "../../common/log.h"
#include "lineairdb.pb.h"

WorkerManager::WorkerManager(std::shared_ptr<DatabaseManager> db_manager,
                             std::shared_ptr<TransactionManager> tx_manager)
    : db_manager_(std::move(db_manager)), tx_manager_(std::move(tx_manager)) {}

std::string WorkerManager::begin_on_new_worker(uint64_t sender_id,
                                               const std::string& payload,
                                               int64_t& out_tx_id) {
    // Create worker first
    auto worker = std::make_unique<TransactionWorker>(db_manager_, tx_manager_);

    // Enqueue Begin on the worker and wait for the response
    std::string response_bytes = worker->enqueue_and_wait(sender_id, MessageType::TX_BEGIN_TRANSACTION, payload);

    // Extract tx_id from response to register mapping
    LineairDB::Protocol::TxBeginTransaction::Response response;
    if (response.ParseFromString(response_bytes)) {
        out_tx_id = response.transaction_id();
        if (out_tx_id > 0) {
            workers_.emplace(out_tx_id, std::move(worker));
        } else {
            LOG_ERROR("BEGIN returned invalid tx_id: %ld", out_tx_id);
        }
    } else {
        out_tx_id = -1;
        LOG_ERROR("Failed to parse BeginTransaction response");
    }
    return response_bytes;
}

std::string WorkerManager::dispatch_to_worker(uint64_t sender_id,
                                              int64_t tx_id,
                                              MessageType type,
                                              const std::string& payload) {
    auto it = workers_.find(tx_id);
    if (it == workers_.end()) {
        LOG_WARNING("No worker found for tx_id=%ld", tx_id);
        return std::string();
    }
    auto response = it->second->enqueue_and_wait(sender_id, type, payload);
    if (type == MessageType::DB_END_TRANSACTION) {
        // After EndTransaction, clean up the worker
        it->second->shutdown();
        workers_.erase(it);
    }
    return response;
}

void WorkerManager::remove_worker(int64_t tx_id) {
    auto it = workers_.find(tx_id);
    if (it != workers_.end()) {
        it->second->shutdown();
        workers_.erase(it);
    }
}
