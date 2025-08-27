#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <errno.h>
#include <cstring>
#include <iostream>
#include <vector>

#include "lineairdb_client.hh"
#include "lineairdb_transaction.hh"
#include "../common/log.h"

LineairDBClient::LineairDBClient() 
    : socket_fd_(-1), connected_(false), host_("127.0.0.1"), port_(9999) {
    if (!connect(host_, port_)) {
        std::cerr << "Failed to connect to LineairDB service" << std::endl;
    }
}

LineairDBClient::~LineairDBClient() {
    disconnect();
}

bool LineairDBClient::connect(const std::string& host, int port) {
    if (connected_) {
        disconnect();
    }
    
    // create TCP socket
    socket_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (socket_fd_ < 0) {
        return false;
    }

    // set up server address
    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    
    if (inet_pton(AF_INET, host.c_str(), &server_addr.sin_addr) <= 0) {
        close(socket_fd_);
        socket_fd_ = -1;
        return false;
    }

    // connect
    if (::connect(socket_fd_, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        close(socket_fd_);
        socket_fd_ = -1;
        return false;
    }
    
    connected_ = true;
    host_ = host;
    port_ = port;
    return true;
}

void LineairDBClient::disconnect() {
    if (socket_fd_ >= 0) {
        close(socket_fd_);
        socket_fd_ = -1;
    }
    connected_ = false;
}

bool LineairDBClient::is_connected() const {
    return connected_;
}

std::string LineairDBClient::tx_read(LineairDBTransaction* tx, const std::string& key) {
    int64_t tx_id = tx->get_tx_id();
    LOG_DEBUG("CLIENT: tx_read called with tx_id=%ld, key=%s", tx_id, key.c_str());
    if (!connected_) {
        LOG_ERROR("RPC failed: Not connected to server");
        return "";
    }

    LineairDB::Protocol::TxRead::Request request;
    LineairDB::Protocol::TxRead::Response response;
    
    request.set_transaction_id(tx_id);
    request.set_key(key);
    LOG_DEBUG("CLIENT: Created read request");

    if (!send_protobuf_message(request, response, MessageType::TX_READ)) {
        LOG_ERROR("RPC failed: Failed to send message to server");
        return "";
    }

    // Update transaction abort status
    tx->set_aborted(response.is_aborted());

    LOG_DEBUG("CLIENT: tx_read completed, found: %s", response.found() ? "true" : "false");
    return response.found() ? response.value() : "";
}

bool LineairDBClient::tx_write(LineairDBTransaction* tx, const std::string& key, const std::string& value) {
    int64_t tx_id = tx->get_tx_id();
    LOG_DEBUG("CLIENT: tx_write called with tx_id=%ld, key=%s, value=%s", tx_id, key.c_str(), value.c_str());
    if (!connected_) {
        LOG_ERROR("RPC failed: Not connected to server");
        return false;
    }

    LineairDB::Protocol::TxWrite::Request request;
    LineairDB::Protocol::TxWrite::Response response;
    
    request.set_transaction_id(tx_id);
    request.set_key(key);
    request.set_value(value);
    LOG_DEBUG("CLIENT: Created write request");

    if (!send_protobuf_message(request, response, MessageType::TX_WRITE)) {
        LOG_ERROR("RPC failed: Failed to send message to server");
        return false;
    }

    // Update transaction abort status
    tx->set_aborted(response.is_aborted());

    LOG_DEBUG("CLIENT: tx_write completed, success: %s", response.success() ? "true" : "false");
    return response.success();
}

std::vector<KeyValue> LineairDBClient::tx_scan(LineairDBTransaction* tx, const std::string& db_table_key, const std::string& first_key_part) {
    int64_t tx_id = tx->get_tx_id();
    LOG_DEBUG("CLIENT: tx_scan called with tx_id=%ld, table=%s, prefix=%s", tx_id, db_table_key.c_str(), first_key_part.c_str());
    if (!connected_) {
        LOG_ERROR("RPC failed: Not connected to server");
        return {};
    }

    LineairDB::Protocol::TxScan::Request request;
    LineairDB::Protocol::TxScan::Response response;
    
    request.set_transaction_id(tx_id);
    request.set_db_table_key(db_table_key);
    request.set_first_key_part(first_key_part);
    LOG_DEBUG("CLIENT: Created scan request");

    if (!send_protobuf_message(request, response, MessageType::TX_SCAN)) {
        LOG_ERROR("RPC failed: Failed to send message to server");
        return {};
    }

    // Update transaction abort status
    tx->set_aborted(response.is_aborted());

    std::vector<KeyValue> key_values;
    for (const auto& kv : response.key_values()) {
        key_values.emplace_back(KeyValue{kv.key(), kv.value()});
        LOG_DEBUG("CLIENT: received key='%s', value_size=%zu", kv.key().c_str(), kv.value().size());
    }
    
    LOG_DEBUG("CLIENT: tx_scan completed, found %zu key-value pairs", key_values.size());
    return key_values;
}

int64_t LineairDBClient::tx_begin_transaction() {
    LOG_DEBUG("CLIENT: tx_begin_transaction called");
    if (!connected_) {
        LOG_ERROR("RPC failed: Not connected to server");
        return -1;
    }

    LineairDB::Protocol::TxBeginTransaction::Request request;
    LineairDB::Protocol::TxBeginTransaction::Response response;
    LOG_DEBUG("CLIENT: Created begin transaction request");

    if (!send_protobuf_message(request, response, MessageType::TX_BEGIN_TRANSACTION)) {
        LOG_ERROR("RPC failed: Failed to send message to server");
        return -1;
    }

    LOG_DEBUG("CLIENT: tx_begin_transaction completed, tx_id: %ld", response.transaction_id());
    return response.transaction_id();
}

void LineairDBClient::tx_abort(int64_t tx_id) {
    LOG_DEBUG("CLIENT: tx_abort called with tx_id=%ld", tx_id);
    if (!connected_) {
        LOG_ERROR("RPC failed: Not connected to server");
        return;
    }

    LineairDB::Protocol::TxAbort::Request request;
    LineairDB::Protocol::TxAbort::Response response;
    
    request.set_transaction_id(tx_id);
    LOG_DEBUG("CLIENT: Created abort request");

    if (!send_protobuf_message(request, response, MessageType::TX_ABORT)) {
        LOG_ERROR("RPC failed: Failed to send message to server");
        return;
    }

    LOG_DEBUG("CLIENT: tx_abort completed");
}

void LineairDBClient::db_end_transaction(int64_t tx_id, bool isFence) {
    LOG_DEBUG("CLIENT: db_end_transaction called with tx_id=%ld, fence=%s", tx_id, isFence ? "true" : "false");
    if (!connected_) {
        LOG_ERROR("RPC failed: Not connected to server");
        return;
    }

    LineairDB::Protocol::DbEndTransaction::Request request;
    LineairDB::Protocol::DbEndTransaction::Response response;
    
    request.set_transaction_id(tx_id);
    request.set_fence(isFence);
    LOG_DEBUG("CLIENT: Created end_transaction request");

    if (!send_protobuf_message(request, response, MessageType::DB_END_TRANSACTION)) {
        LOG_ERROR("RPC failed: Failed to send message to server");
        return;
    }

    LOG_DEBUG("CLIENT: db_end_transaction completed");
}

bool LineairDBClient::tx_is_aborted(int64_t tx_id) {
    LOG_DEBUG("CLIENT: tx_is_aborted called with tx_id=%ld", tx_id);
    if (!connected_) {
        LOG_ERROR("RPC failed: Not connected to server");
        return true;  // assume aborted if not connected
    }

    LineairDB::Protocol::TxIsAborted::Request request;
    LineairDB::Protocol::TxIsAborted::Response response;
    
    request.set_transaction_id(tx_id);
    LOG_DEBUG("CLIENT: Created is_aborted request");

    if (!send_protobuf_message(request, response, MessageType::TX_IS_ABORTED)) {
        LOG_ERROR("RPC failed: Failed to send message to server");
        return true;  // assume aborted on failure
    }

    LOG_DEBUG("CLIENT: tx_is_aborted completed, result: %s", response.is_aborted() ? "true" : "false");
    return response.is_aborted();
}

void LineairDBClient::db_fence() {
    LOG_DEBUG("CLIENT: db_fence called");
    if (!connected_) {
        LOG_ERROR("RPC failed: Not connected to server");
        return;
    }

    LineairDB::Protocol::DbFence::Request request;
    LineairDB::Protocol::DbFence::Response response;
    LOG_DEBUG("CLIENT: Created fence request");

    if (!send_protobuf_message(request, response, MessageType::DB_FENCE)) {
        LOG_ERROR("RPC failed: Failed to send message to server");
        return;
    }

    LOG_DEBUG("CLIENT: db_fence completed");
}

bool LineairDBClient::send_message(const std::string& serialized_request, std::string& serialized_response) {
    if (!connected_) {
        LOG_ERROR("SEND_MESSAGE: Not connected to server");
        return false;
    }
    
    LOG_DEBUG("SEND_MESSAGE: Sending message of size %zu bytes", serialized_request.size());
    
    // send message size first (4 bytes)
    uint32_t message_size = htonl(serialized_request.size());
    LOG_DEBUG("SEND_MESSAGE: Sending size header: %zu (network order: %u)", serialized_request.size(), message_size);
    
    ssize_t size_sent = send(socket_fd_, &message_size, sizeof(message_size), 0);
    if (size_sent != sizeof(message_size)) {
        LOG_ERROR("SEND_MESSAGE: Failed to send size header, sent %zd bytes instead of %zu", size_sent, sizeof(message_size));
        return false;
    }
    LOG_DEBUG("SEND_MESSAGE: Size header sent successfully");

    // send message body
    LOG_DEBUG("SEND_MESSAGE: Sending message body...");
    ssize_t body_sent = send(socket_fd_, serialized_request.data(), serialized_request.size(), 0);
    if (body_sent != static_cast<ssize_t>(serialized_request.size())) {
        LOG_ERROR("SEND_MESSAGE: Failed to send message body, sent %zd bytes instead of %zu", body_sent, serialized_request.size());
        return false;
    }
    LOG_DEBUG("SEND_MESSAGE: Message body sent successfully");

    // receive response size
    LOG_DEBUG("SEND_MESSAGE: Waiting for response size...");
    uint32_t response_size;
    ssize_t size_received = recv(socket_fd_, &response_size, sizeof(response_size), MSG_WAITALL);
    if (size_received != sizeof(response_size)) {
        LOG_ERROR("SEND_MESSAGE: Failed to receive response size, got %zd bytes", size_received);
        return false;
    }
    response_size = ntohl(response_size);
    LOG_DEBUG("SEND_MESSAGE: Received response size: %u bytes", response_size);

    // receive response body
    LOG_DEBUG("SEND_MESSAGE: Waiting for response body...");
    serialized_response.resize(response_size);
    ssize_t body_received = recv(socket_fd_, &serialized_response[0], response_size, MSG_WAITALL);
    if (body_received != static_cast<ssize_t>(response_size)) {
        LOG_ERROR("SEND_MESSAGE: Failed to receive response body, got %zd bytes instead of %u", body_received, response_size);
        return false;
    }
    LOG_DEBUG("SEND_MESSAGE: Response body received successfully");
    
    return true;
}

template<typename RequestType, typename ResponseType>
bool LineairDBClient::send_protobuf_message(const RequestType& request, ResponseType& response, MessageType message_type) {
    LOG_DEBUG("PROTOBUF_MESSAGE: Starting protobuf message send");
    
    // serialize request
    std::string serialized_request = request.SerializeAsString();
    LOG_DEBUG("PROTOBUF_MESSAGE: Request serialized successfully, size: %zu bytes", serialized_request.size());

    // send message with header
    std::string serialized_response;
    if (!send_message_with_header(serialized_request, serialized_response, message_type)) {
        LOG_ERROR("PROTOBUF_MESSAGE: Failed to send message with header");
        return false;
    }

    // deserialize response
    if (!response.ParseFromString(serialized_response)) {
        LOG_ERROR("PROTOBUF_MESSAGE: Failed to parse response");
        return false;
    }
    
    LOG_DEBUG("PROTOBUF_MESSAGE: Successfully completed protobuf message exchange");
    return true;
}

bool LineairDBClient::send_message_with_header(const std::string& serialized_request, std::string& serialized_response, MessageType message_type) {
    if (!connected_) {
        LOG_ERROR("SEND_MESSAGE: Not connected!");
        return false;
    }
    
    LOG_DEBUG("SEND_MESSAGE: Sending message of size %zu bytes with message_type %u", serialized_request.size(), static_cast<uint32_t>(message_type));

    // prepare message header
    MessageHeader header;
    header.sender_id = htobe64(1);  // TODO: replace with actual sender ID
    header.message_type = htonl(static_cast<uint32_t>(message_type));
    header.payload_size = htonl(static_cast<uint32_t>(serialized_request.size()));
    
    LOG_DEBUG("SEND_MESSAGE: Prepared header: sender_id=1, message_type=%u, payload_size=%zu", static_cast<uint32_t>(message_type), serialized_request.size());
    
    // combine header and payload
    size_t total_size = sizeof(header) + serialized_request.size();
    std::vector<char> buffer(total_size);
    std::memcpy(buffer.data(), &header, sizeof(header));
    std::memcpy(buffer.data() + sizeof(header), serialized_request.c_str(), serialized_request.size());
    
    // send
    ssize_t bytes_sent = send(socket_fd_, buffer.data(), total_size, 0);
    if (bytes_sent != static_cast<ssize_t>(total_size)) {
        LOG_ERROR("SEND_MESSAGE: Failed to send complete message, sent %zd bytes instead of %zu", bytes_sent, total_size);
        return false;
    }
    
    LOG_DEBUG("SEND_MESSAGE: Successfully sent %zd bytes", bytes_sent);

    // receive response header
    MessageHeader response_header;
    ssize_t header_received = recv(socket_fd_, &response_header, sizeof(response_header), MSG_WAITALL);
    if (header_received != sizeof(response_header)) {
        LOG_ERROR("SEND_MESSAGE: Failed to receive response header, received %zd bytes", header_received);
        return false;
    }

    // convert from network byte order to host byte order
    uint64_t response_sender_id = be64toh(response_header.sender_id);
    uint32_t response_message_type = ntohl(response_header.message_type);
    uint32_t response_payload_size = ntohl(response_header.payload_size);
    
    LOG_DEBUG("SEND_MESSAGE: Received response header: sender_id=%lu, message_type=%u, payload_size=%u", response_sender_id, response_message_type, response_payload_size);

    // receive response payload
    if (response_payload_size > 0) {
        serialized_response.resize(response_payload_size);
        ssize_t payload_received = recv(socket_fd_, &serialized_response[0], response_payload_size, MSG_WAITALL);
        if (payload_received != static_cast<ssize_t>(response_payload_size)) {
            LOG_ERROR("SEND_MESSAGE: Failed to receive response payload, received %zd bytes instead of %u", payload_received, response_payload_size);
            return false;
        }
        LOG_DEBUG("SEND_MESSAGE: Successfully received response payload (%zd bytes)", payload_received);
    } else {
        LOG_DEBUG("SEND_MESSAGE: No response payload (empty response)");
        serialized_response.clear();
    }
    
    LOG_DEBUG("SEND_MESSAGE: Message exchange completed successfully");
    return true;
}
