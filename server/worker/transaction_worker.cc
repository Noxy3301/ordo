#include "transaction_worker.hh"
#include "../../common/log.h"

TransactionWorker::TransactionWorker(std::shared_ptr<DatabaseManager> db_manager,
                                     std::shared_ptr<TransactionManager> tx_manager)
    : db_manager_(std::move(db_manager)),
      tx_manager_(std::move(tx_manager)) {
    rpc_ = std::make_unique<LineairDBRpc>(db_manager_, tx_manager_);
    worker_thread_ = std::thread([this]() { this->run(); });
}

TransactionWorker::~TransactionWorker() {
    shutdown();
    if (worker_thread_.joinable()) worker_thread_.join();
}

std::string TransactionWorker::enqueue_and_wait(uint64_t sender_id,
                                                MessageType type,
                                                const std::string& payload) {
    auto response_slot = std::make_shared<ResponseSlot>();
    {
        std::lock_guard<std::mutex> queue_lock(queue_mutex_);
        task_queue_.push(Task{sender_id, type, payload, response_slot});
    }
    queue_cv_.notify_one();

    std::unique_lock<std::mutex> response_lock(response_slot->mutex);
    response_slot->cv.wait(response_lock, [&]{ return response_slot->ready; });
    return std::move(response_slot->data);
}

void TransactionWorker::shutdown() {
    {
        std::lock_guard<std::mutex> queue_lock(queue_mutex_);
        stopping_ = true;
    }
    queue_cv_.notify_one();
}

void TransactionWorker::run() {
    for (;;) {
        Task task;
        {
            std::unique_lock<std::mutex> queue_lock(queue_mutex_);
            queue_cv_.wait(queue_lock, [&]{ return stopping_ || !task_queue_.empty(); });
            if (stopping_ && task_queue_.empty()) break;
            task = std::move(task_queue_.front());
            task_queue_.pop();
        }

        std::string result;
        // Process the RPC
        rpc_->handle_rpc(task.sender_id, task.message_type, task.payload, result);
        {
            std::lock_guard<std::mutex> response_guard(task.response->mutex);
            task.response->data = std::move(result);
            task.response->ready = true;
        }
        task.response->cv.notify_all();

        // If this was EndTransaction, we can stop the worker
        if (task.message_type == MessageType::DB_END_TRANSACTION) {
            // drain any queued items as failed (should not happen)
            std::lock_guard<std::mutex> queue_lock(queue_mutex_);
            while (!task_queue_.empty()) {
                auto extra = std::move(task_queue_.front());
                task_queue_.pop();
                std::lock_guard<std::mutex> response_guard(extra.response->mutex);
                extra.response->data.clear();
                extra.response->ready = true;
                extra.response->cv.notify_all();
            }
            stopping_ = true;
        }
    }
}
