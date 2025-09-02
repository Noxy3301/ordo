#pragma once

#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>

#include "../protocol/message.hh"
#include "../rpc/lineairdb_rpc.hh"

// A single-transaction worker: processes all RPCs for one transaction
class TransactionWorker {
public:
    struct ResponseSlot {
        std::mutex mutex;
        std::condition_variable cv;
        bool ready = false;
        std::string data;
    };

    struct Task {
        uint64_t sender_id;
        MessageType message_type;
        std::string payload;
        std::shared_ptr<ResponseSlot> response;
    };

    TransactionWorker(std::shared_ptr<DatabaseManager> db_manager,
                      std::shared_ptr<TransactionManager> tx_manager);
    ~TransactionWorker();

    // Enqueue a task and synchronously wait for the serialized response
    std::string enqueue_and_wait(uint64_t sender_id,
                                 MessageType type,
                                 const std::string& payload);

    // Signal the worker to stop after draining
    void shutdown();

private:
    void run();

    // Queue and synchronization primitives
    std::mutex queue_mutex_;
    std::condition_variable queue_cv_;
    std::queue<Task> task_queue_;
    bool stopping_ = false;
    std::thread worker_thread_;

    std::shared_ptr<DatabaseManager> db_manager_;
    std::shared_ptr<TransactionManager> tx_manager_;
    std::unique_ptr<LineairDBRpc> rpc_;
};
