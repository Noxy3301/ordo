#include "batch_dispatcher.hh"

#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>
#include <chrono>

#include "../common/log.h"

// ============================================================================
// BatchBuffer implementation
// ============================================================================

BatchBuffer::BatchBuffer() {
    operations_.reserve(MAX_OPERATIONS);
}

uint32_t BatchBuffer::add(BatchOpType type, const std::string& serialized_request,
                          std::promise<std::string> promise) {
    std::lock_guard<std::mutex> lock(mutex_);

    uint32_t op_id = next_operation_id_++;
    operations_.push_back({
        op_id,
        type,
        serialized_request,
        std::move(promise)
    });

    return op_id;
}

bool BatchBuffer::should_dispatch() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return operations_.size() >= MAX_OPERATIONS;
}

bool BatchBuffer::empty() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return operations_.empty();
}

size_t BatchBuffer::size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return operations_.size();
}

std::string BatchBuffer::build_request() const {
    std::lock_guard<std::mutex> lock(mutex_);

    LineairDB::Protocol::TxBatchOperations::Request batch_request;

    for (const auto& pending_op : operations_) {
        auto* op = batch_request.add_operations();
        op->set_operation_id(pending_op.operation_id);

        switch (pending_op.type) {
            case BatchOpType::READ: {
                LineairDB::Protocol::TxRead::Request read_req;
                read_req.ParseFromString(pending_op.serialized_request);
                *op->mutable_read() = read_req;
                break;
            }
            case BatchOpType::WRITE: {
                LineairDB::Protocol::TxWrite::Request write_req;
                write_req.ParseFromString(pending_op.serialized_request);
                *op->mutable_write() = write_req;
                break;
            }
            case BatchOpType::SCAN: {
                LineairDB::Protocol::TxScan::Request scan_req;
                scan_req.ParseFromString(pending_op.serialized_request);
                *op->mutable_scan() = scan_req;
                break;
            }
            case BatchOpType::END_TRANSACTION: {
                LineairDB::Protocol::DbEndTransaction::Request end_req;
                end_req.ParseFromString(pending_op.serialized_request);
                *op->mutable_end_transaction() = end_req;
                break;
            }
            case BatchOpType::BEGIN_TRANSACTION: {
                LineairDB::Protocol::TxBeginTransaction::Request begin_req;
                begin_req.ParseFromString(pending_op.serialized_request);
                *op->mutable_begin_transaction() = begin_req;
                break;
            }
            case BatchOpType::ABORT: {
                LineairDB::Protocol::TxAbort::Request abort_req;
                abort_req.ParseFromString(pending_op.serialized_request);
                *op->mutable_abort() = abort_req;
                break;
            }
        }
    }

    return batch_request.SerializeAsString();
}

void BatchBuffer::distribute_results(const std::string& serialized_response) {
    std::lock_guard<std::mutex> lock(mutex_);

    LineairDB::Protocol::TxBatchOperations::Response batch_response;
    if (!batch_response.ParseFromString(serialized_response)) {
        LOG_ERROR("Failed to parse batch response");
        // Set error for all promises
        for (auto& pending_op : operations_) {
            pending_op.result_promise.set_value("");
        }
        return;
    }

    // Build map from operation_id to result
    std::unordered_map<uint32_t, const LineairDB::Protocol::TxBatchOperations::OperationResult*> results_map;
    for (const auto& result : batch_response.results()) {
        results_map[result.operation_id()] = &result;
    }

    // Distribute results to promises
    for (auto& pending_op : operations_) {
        auto it = results_map.find(pending_op.operation_id);
        if (it == results_map.end()) {
            LOG_ERROR("Missing result for operation_id=%u", pending_op.operation_id);
            pending_op.result_promise.set_value("");
            continue;
        }

        const auto* result = it->second;
        std::string serialized_result;

        switch (pending_op.type) {
            case BatchOpType::READ:
                serialized_result = result->read().SerializeAsString();
                break;
            case BatchOpType::WRITE:
                serialized_result = result->write().SerializeAsString();
                break;
            case BatchOpType::SCAN:
                serialized_result = result->scan().SerializeAsString();
                break;
            case BatchOpType::END_TRANSACTION:
                serialized_result = result->end_transaction().SerializeAsString();
                break;
            case BatchOpType::BEGIN_TRANSACTION:
                serialized_result = result->begin_transaction().SerializeAsString();
                break;
            case BatchOpType::ABORT:
                serialized_result = result->abort().SerializeAsString();
                break;
        }

        pending_op.result_promise.set_value(serialized_result);
    }
}

void BatchBuffer::reset() {
    std::lock_guard<std::mutex> lock(mutex_);
    operations_.clear();
    next_operation_id_ = 0;
}

// ============================================================================
// BatchDispatcher implementation
// ============================================================================

BatchDispatcher::BatchDispatcher(const std::string& host, int port)
    : socket_fd_(-1), host_(host), port_(port), current_batch_idx_(0) {

    // Initialize batch pool
    for (size_t i = 0; i < POOL_SIZE; ++i) {
        batches_[i] = std::make_unique<BatchBuffer>();
        available_batches_.push(i);
    }

    // Take first batch as current
    current_batch_idx_ = available_batches_.front();
    available_batches_.pop();

    // Connect to server
    if (!connect_to_server()) {
        LOG_ERROR("BatchDispatcher: Failed to connect to %s:%d", host_.c_str(), port_);
    }

    // Start epoch timer thread
    epoch_thread_ = std::thread(&BatchDispatcher::epoch_timer_thread, this);

    LOG_INFO("BatchDispatcher initialized: host=%s, port=%d, pool_size=%zu, epoch_ms=%u",
             host_.c_str(), port_, POOL_SIZE, epoch_ms_.load());
}

BatchDispatcher::~BatchDispatcher() {
    running_ = false;

    if (epoch_thread_.joinable()) {
        epoch_thread_.join();
    }

    // Flush any remaining operations
    flush();

    disconnect();

    LOG_INFO("BatchDispatcher destroyed: batches_dispatched=%lu, operations_batched=%lu",
             batches_dispatched_.load(), operations_batched_.load());
}

bool BatchDispatcher::connect_to_server() {
    if (connected_) {
        return true;
    }

    socket_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (socket_fd_ < 0) {
        LOG_ERROR("Failed to create socket");
        return false;
    }

    // Set TCP_NODELAY for low latency
    int flag = 1;
    setsockopt(socket_fd_, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));

    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port_);

    if (inet_pton(AF_INET, host_.c_str(), &server_addr.sin_addr) <= 0) {
        LOG_ERROR("Invalid address: %s", host_.c_str());
        close(socket_fd_);
        socket_fd_ = -1;
        return false;
    }

    if (::connect(socket_fd_, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        LOG_ERROR("Failed to connect to %s:%d", host_.c_str(), port_);
        close(socket_fd_);
        socket_fd_ = -1;
        return false;
    }

    connected_ = true;
    return true;
}

void BatchDispatcher::disconnect() {
    if (socket_fd_ >= 0) {
        close(socket_fd_);
        socket_fd_ = -1;
    }
    connected_ = false;
}

std::string BatchDispatcher::submit_read(int64_t tx_id, const std::string& key) {
    LineairDB::Protocol::TxRead::Request request;
    request.set_transaction_id(tx_id);
    request.set_key(key);

    auto future = submit_operation(BatchOpType::READ, request.SerializeAsString());
    return future.get();  // Block until result
}

std::string BatchDispatcher::submit_write(int64_t tx_id, const std::string& key, const std::string& value) {
    LineairDB::Protocol::TxWrite::Request request;
    request.set_transaction_id(tx_id);
    request.set_key(key);
    request.set_value(value);

    auto future = submit_operation(BatchOpType::WRITE, request.SerializeAsString());
    return future.get();  // Block until result
}

std::string BatchDispatcher::submit_scan(int64_t tx_id, const std::string& db_table_key, const std::string& first_key_part) {
    LineairDB::Protocol::TxScan::Request request;
    request.set_transaction_id(tx_id);
    request.set_db_table_key(db_table_key);
    request.set_first_key_part(first_key_part);

    auto future = submit_operation(BatchOpType::SCAN, request.SerializeAsString());
    return future.get();  // Block until result
}

std::string BatchDispatcher::submit_end_transaction(int64_t tx_id, bool fence) {
    LineairDB::Protocol::DbEndTransaction::Request request;
    request.set_transaction_id(tx_id);
    request.set_fence(fence);

    auto future = submit_operation(BatchOpType::END_TRANSACTION, request.SerializeAsString());
    return future.get();  // Block until result
}

std::string BatchDispatcher::submit_begin_transaction() {
    LineairDB::Protocol::TxBeginTransaction::Request request;
    // Request is empty for begin_transaction

    auto future = submit_operation(BatchOpType::BEGIN_TRANSACTION, request.SerializeAsString());
    return future.get();  // Block until result
}

std::string BatchDispatcher::submit_abort(int64_t tx_id) {
    LineairDB::Protocol::TxAbort::Request request;
    request.set_transaction_id(tx_id);

    auto future = submit_operation(BatchOpType::ABORT, request.SerializeAsString());
    return future.get();  // Block until result
}

std::future<std::string> BatchDispatcher::submit_operation(BatchOpType type, const std::string& serialized_request) {
    std::promise<std::string> promise;
    auto future = promise.get_future();

    size_t batch_to_dispatch = SIZE_MAX;

    {
        std::unique_lock<std::mutex> lock(pool_mutex_);

        // Add to current batch
        batches_[current_batch_idx_]->add(type, serialized_request, std::move(promise));
        operations_batched_++;

        // Check if we should dispatch
        if (batches_[current_batch_idx_]->should_dispatch()) {
            batch_to_dispatch = current_batch_idx_;

            // Get next available batch
            if (!available_batches_.empty()) {
                current_batch_idx_ = available_batches_.front();
                available_batches_.pop();
            } else {
                // All batches in use, wait for one to become available
                // This shouldn't happen often if POOL_SIZE is sufficient
                LOG_WARNING("BatchDispatcher: All batches in use, waiting...");
                pool_cv_.wait(lock, [this] { return !available_batches_.empty(); });
                current_batch_idx_ = available_batches_.front();
                available_batches_.pop();
            }
        }
    }

    // Dispatch outside the lock
    if (batch_to_dispatch != SIZE_MAX) {
        dispatch_batch(batch_to_dispatch);
    }

    return future;
}

void BatchDispatcher::dispatch_batch(size_t batch_idx) {
    if (batches_[batch_idx]->empty()) {
        release_batch(batch_idx);
        return;
    }

    std::string request = batches_[batch_idx]->build_request();
    std::string response;

    {
        std::lock_guard<std::mutex> lock(dispatch_mutex_);

        if (!send_batch_request(request, response)) {
            LOG_ERROR("BatchDispatcher: Failed to send batch request");
            // Promises will be fulfilled with empty strings by distribute_results
        }
    }

    batches_[batch_idx]->distribute_results(response);
    batches_dispatched_++;

    // Reset and return to pool
    batches_[batch_idx]->reset();
    release_batch(batch_idx);
}

bool BatchDispatcher::send_batch_request(const std::string& request, std::string& response) {
    if (!connected_) {
        if (!connect_to_server()) {
            return false;
        }
    }

    // Prepare header
    struct {
        uint64_t sender_id;
        uint32_t message_type;
        uint32_t payload_size;
    } header;

    header.sender_id = htobe64(1);
    header.message_type = htonl(static_cast<uint32_t>(MessageType::TX_BATCH_OPERATIONS));
    header.payload_size = htonl(static_cast<uint32_t>(request.size()));

    // Send header + payload
    size_t total_size = sizeof(header) + request.size();
    std::vector<char> buffer(total_size);
    std::memcpy(buffer.data(), &header, sizeof(header));
    std::memcpy(buffer.data() + sizeof(header), request.data(), request.size());

    ssize_t sent = send(socket_fd_, buffer.data(), total_size, 0);
    if (sent != static_cast<ssize_t>(total_size)) {
        LOG_ERROR("Failed to send batch request: sent=%zd, expected=%zu", sent, total_size);
        connected_ = false;
        return false;
    }

    // Receive response header
    struct {
        uint64_t sender_id;
        uint32_t message_type;
        uint32_t payload_size;
    } response_header;

    ssize_t received = recv(socket_fd_, &response_header, sizeof(response_header), MSG_WAITALL);
    if (received != sizeof(response_header)) {
        LOG_ERROR("Failed to receive response header");
        connected_ = false;
        return false;
    }

    uint32_t payload_size = ntohl(response_header.payload_size);

    // Receive response payload
    if (payload_size > 0) {
        response.resize(payload_size);
        received = recv(socket_fd_, &response[0], payload_size, MSG_WAITALL);
        if (received != static_cast<ssize_t>(payload_size)) {
            LOG_ERROR("Failed to receive response payload");
            connected_ = false;
            return false;
        }
    }

    return true;
}

void BatchDispatcher::epoch_timer_thread() {
    while (running_) {
        std::this_thread::sleep_for(std::chrono::milliseconds(epoch_ms_.load()));

        if (!running_ || !enabled_) {
            continue;
        }

        size_t batch_to_dispatch = SIZE_MAX;

        {
            std::lock_guard<std::mutex> lock(pool_mutex_);

            if (!batches_[current_batch_idx_]->empty()) {
                batch_to_dispatch = current_batch_idx_;

                // Get next available batch
                if (!available_batches_.empty()) {
                    current_batch_idx_ = available_batches_.front();
                    available_batches_.pop();
                }
                // If no batch available, we'll just dispatch and wait
            }
        }

        if (batch_to_dispatch != SIZE_MAX) {
            dispatch_batch(batch_to_dispatch);
        }
    }
}

size_t BatchDispatcher::acquire_batch() {
    std::unique_lock<std::mutex> lock(pool_mutex_);
    pool_cv_.wait(lock, [this] { return !available_batches_.empty(); });

    size_t idx = available_batches_.front();
    available_batches_.pop();
    return idx;
}

void BatchDispatcher::release_batch(size_t idx) {
    std::lock_guard<std::mutex> lock(pool_mutex_);
    available_batches_.push(idx);
    pool_cv_.notify_one();
}

void BatchDispatcher::flush() {
    size_t batch_to_dispatch = SIZE_MAX;

    {
        std::lock_guard<std::mutex> lock(pool_mutex_);

        if (!batches_[current_batch_idx_]->empty()) {
            batch_to_dispatch = current_batch_idx_;

            if (!available_batches_.empty()) {
                current_batch_idx_ = available_batches_.front();
                available_batches_.pop();
            }
        }
    }

    if (batch_to_dispatch != SIZE_MAX) {
        dispatch_batch(batch_to_dispatch);
    }
}

void BatchDispatcher::set_epoch_ms(uint32_t ms) {
    epoch_ms_ = ms;
}

void BatchDispatcher::set_enabled(bool enabled) {
    enabled_ = enabled;
}

bool BatchDispatcher::is_enabled() const {
    return enabled_;
}

uint64_t BatchDispatcher::get_batches_dispatched() const {
    return batches_dispatched_;
}

uint64_t BatchDispatcher::get_operations_batched() const {
    return operations_batched_;
}
