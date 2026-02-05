#ifndef BATCH_DISPATCHER_H
#define BATCH_DISPATCHER_H

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <vector>

#include "lineairdb.pb.h"
#include "lineairdb_client.hh"

// Forward declaration
class LineairDBClient;

// Operation type for batch operations
enum class BatchOpType {
    READ,
    WRITE,
    SCAN,
    END_TRANSACTION,
    BEGIN_TRANSACTION,
    ABORT
};

// Pending operation waiting for batch dispatch
struct PendingOperation {
    uint32_t operation_id;
    BatchOpType type;
    std::string serialized_request;  // serialized protobuf request
    std::promise<std::string> result_promise;  // fulfilled when result arrives
};

// A single batch buffer that accumulates operations
class BatchBuffer {
public:
    static constexpr size_t MAX_OPERATIONS = 1000;

    BatchBuffer();
    ~BatchBuffer() = default;

    // Add operation to this batch, returns operation_id
    uint32_t add(BatchOpType type, const std::string& serialized_request,
                 std::promise<std::string> promise);

    // Check if batch should be dispatched
    bool should_dispatch() const;

    // Check if batch is empty
    bool empty() const;

    // Get number of operations
    size_t size() const;

    // Build batch request protobuf
    std::string build_request() const;

    // Distribute results to waiting promises
    void distribute_results(const std::string& serialized_response);

    // Reset batch for reuse
    void reset();

private:
    std::vector<PendingOperation> operations_;
    std::atomic<uint32_t> next_operation_id_{0};
    mutable std::mutex mutex_;
};

// Pool of batch buffers with epoch-based dispatching
class BatchDispatcher {
public:
    static constexpr size_t POOL_SIZE = 4;
    static constexpr uint32_t DEFAULT_EPOCH_US = 10;  // 10μs epoch

    BatchDispatcher(const std::string& host, int port);
    ~BatchDispatcher();

    // Submit operation and block until result is ready
    // Returns serialized response protobuf
    std::string submit_read(int64_t tx_id, const std::string& key);
    std::string submit_write(int64_t tx_id, const std::string& key, const std::string& value);
    std::string submit_scan(int64_t tx_id, const std::string& db_table_key, const std::string& first_key_part);
    std::string submit_end_transaction(int64_t tx_id, bool fence);
    std::string submit_begin_transaction();
    std::string submit_abort(int64_t tx_id);

    // Control methods
    void set_epoch_us(uint32_t us);
    void set_enabled(bool enabled);
    bool is_enabled() const;

    // Force flush current batch (used before commit)
    void flush();

    // Statistics
    uint64_t get_batches_dispatched() const;
    uint64_t get_operations_batched() const;

private:
    // Submit generic operation
    std::future<std::string> submit_operation(BatchOpType type, const std::string& serialized_request);

    // Dispatch a batch buffer
    void dispatch_batch(size_t batch_idx);

    // Epoch timer thread
    void epoch_timer_thread();

    // Get next available batch buffer from pool
    size_t acquire_batch();

    // Return batch buffer to pool
    void release_batch(size_t idx);

    // Network connection
    int socket_fd_;
    std::string host_;
    int port_;
    std::atomic<bool> connected_{false};

    // Batch pool
    std::array<std::unique_ptr<BatchBuffer>, POOL_SIZE> batches_;
    std::queue<size_t> available_batches_;  // indices of available batches
    size_t current_batch_idx_;
    std::mutex pool_mutex_;
    std::condition_variable pool_cv_;

    // Epoch timer
    std::thread epoch_thread_;
    std::atomic<bool> running_{true};
    std::atomic<uint32_t> epoch_us_{DEFAULT_EPOCH_US};

    // Enable/disable batching
    std::atomic<bool> enabled_{true};

    // Dispatch lock (only one dispatch at a time per socket)
    std::mutex dispatch_mutex_;

    // Statistics
    std::atomic<uint64_t> batches_dispatched_{0};
    std::atomic<uint64_t> operations_batched_{0};

    // Network helpers
    bool connect_to_server();
    void disconnect();
    bool send_batch_request(const std::string& request, std::string& response);
};

#endif // BATCH_DISPATCHER_H
