#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <errno.h>
#include <cstring>
#include <iostream>
#include <vector>

#include "lineairdb_client.hh"
#include "lineairdb.pb.h"

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

std::string LineairDBClient::tx_read(int64_t tx_id, const std::string& key) {
    std::cout << "CLIENT: tx_read called with tx_id=" << tx_id << ", key=" << key << std::endl;
    if (!connected_) {
        std::cout << "RPC failed[-]: " << "Not connected to lineairdb_service" << std::endl;
        return "";
    }

    LineairDB::Protocol::TxRead::Request request;
    LineairDB::Protocol::TxRead::Response response;
    
    request.set_transaction_id(tx_id);
    request.set_key(key);
    std::cout << "CLIENT: Created read request" << std::endl;

    if (!send_protobuf_message(request, response, MessageType::TX_READ)) {
        std::cout << "RPC failed[-]: " << "Failed to send message to lineairdb_service" << std::endl;
        return "";
    }

    std::cout << "CLIENT: tx_read completed, found: " << response.found() << std::endl;
    return response.found() ? response.value() : "";
}

bool LineairDBClient::tx_write(int64_t tx_id, const std::string& key, const std::string& value) {
    std::cout << "CLIENT: tx_write called with tx_id=" << tx_id << ", key=" << key << ", value=" << value << std::endl;
    if (!connected_) {
        std::cout << "RPC failed[-]: " << "Not connected to lineairdb_service" << std::endl;
        return false;
    }

    LineairDB::Protocol::TxWrite::Request request;
    LineairDB::Protocol::TxWrite::Response response;
    
    request.set_transaction_id(tx_id);
    request.set_key(key);
    request.set_value(value);
    std::cout << "CLIENT: Created write request" << std::endl;

    if (!send_protobuf_message(request, response, MessageType::TX_WRITE)) {
        std::cout << "RPC failed[-]: " << "Failed to send message to lineairdb_service" << std::endl;
        return false;
    }

    std::cout << "CLIENT: tx_write completed, success: " << response.success() << std::endl;
    return response.success();
}

std::vector<std::string> LineairDBClient::tx_scan(int64_t tx_id, const std::string& db_table_key, const std::string& first_key_part) {
    std::cout << "CLIENT: tx_scan called with tx_id=" << tx_id << ", table=" << db_table_key << ", prefix=" << first_key_part << std::endl;
    if (!connected_) {
        std::cout << "RPC failed[-]: " << "Not connected to lineairdb_service" << std::endl;
        return {};
    }

    LineairDB::Protocol::TxScan::Request request;
    LineairDB::Protocol::TxScan::Response response;
    
    request.set_transaction_id(tx_id);
    request.set_db_table_key(db_table_key);
    request.set_first_key_part(first_key_part);
    std::cout << "CLIENT: Created scan request" << std::endl;

    if (!send_protobuf_message(request, response, MessageType::TX_SCAN)) {
        std::cout << "RPC failed[-]: " << "Failed to send message to lineairdb_service" << std::endl;
        return {};
    }

    std::vector<std::string> keys;
    for (const auto& key : response.keys()) {
        keys.push_back(key);
    }
    
    std::cout << "CLIENT: tx_scan completed, found " << keys.size() << " keys" << std::endl;
    return keys;
}

int64_t LineairDBClient::tx_begin_transaction() {
    std::cout << "CLIENT: tx_begin_transaction called" << std::endl;
    if (!connected_) {
        std::cout << "RPC failed[-]: " << "Not connected to lineairdb_service" << std::endl;
        return -1;
    }

    LineairDB::Protocol::TxBeginTransaction::Request request;
    LineairDB::Protocol::TxBeginTransaction::Response response;
    std::cout << "CLIENT: Created begin transaction request" << std::endl;

    if (!send_protobuf_message(request, response, MessageType::TX_BEGIN_TRANSACTION)) {
        std::cout << "RPC failed[-]: " << "Failed to send message to lineairdb_service" << std::endl;
        return -1;
    }

    std::cout << "CLIENT: tx_begin_transaction completed, tx_id: " << response.transaction_id() << std::endl;
    return response.transaction_id();
}

void LineairDBClient::tx_abort(int64_t tx_id) {
    std::cout << "CLIENT: tx_abort called with tx_id=" << tx_id << std::endl;
    if (!connected_) {
        std::cout << "RPC failed[-]: " << "Not connected to lineairdb_service" << std::endl;
        return;
    }

    LineairDB::Protocol::TxAbort::Request request;
    LineairDB::Protocol::TxAbort::Response response;
    
    request.set_transaction_id(tx_id);
    std::cout << "CLIENT: Created abort request" << std::endl;

    if (!send_protobuf_message(request, response, MessageType::TX_ABORT)) {
        std::cout << "RPC failed[-]: " << "Failed to send message to lineairdb_service" << std::endl;
        return;
    }

    std::cout << "CLIENT: tx_abort completed" << std::endl;
}

void LineairDBClient::db_end_transaction(int64_t tx_id, bool isFence) {
    std::cout << "CLIENT: db_end_transaction called with tx_id=" << tx_id << ", fence=" << isFence << std::endl;
    if (!connected_) {
        std::cout << "RPC failed[-]: " << "Not connected to lineairdb_service" << std::endl;
        return;
    }

    LineairDB::Protocol::DbEndTransaction::Request request;
    LineairDB::Protocol::DbEndTransaction::Response response;
    
    request.set_transaction_id(tx_id);
    request.set_fence(isFence);
    std::cout << "CLIENT: Created end_transaction request" << std::endl;

    if (!send_protobuf_message(request, response, MessageType::DB_END_TRANSACTION)) {
        std::cout << "RPC failed[-]: " << "Failed to send message to lineairdb_service" << std::endl;
        return;
    }

    std::cout << "CLIENT: db_end_transaction completed" << std::endl;
}

bool LineairDBClient::tx_is_aborted(int64_t tx_id) {
    std::cout << "CLIENT: tx_is_aborted called with tx_id=" << tx_id << std::endl;
    if (!connected_) {
        std::cout << "RPC failed[-]: " << "Not connected to lineairdb_service" << std::endl;
        return true;  // assume aborted if not connected
    }

    LineairDB::Protocol::TxIsAborted::Request request;
    LineairDB::Protocol::TxIsAborted::Response response;
    
    request.set_transaction_id(tx_id);
    std::cout << "CLIENT: Created is_aborted request" << std::endl;

    if (!send_protobuf_message(request, response, MessageType::TX_IS_ABORTED)) {
        std::cout << "RPC failed[-]: " << "Failed to send message to lineairdb_service" << std::endl;
        return true;  // assume aborted on failure
    }

    std::cout << "CLIENT: tx_is_aborted completed, result: " << response.is_aborted() << std::endl;
    return response.is_aborted();
}

void LineairDBClient::db_fence() {
    std::cout << "CLIENT: db_fence called" << std::endl;
    if (!connected_) {
        std::cout << "RPC failed[-]: " << "Not connected to lineairdb_service" << std::endl;
        return;
    }

    LineairDB::Protocol::DbFence::Request request;
    LineairDB::Protocol::DbFence::Response response;
    std::cout << "CLIENT: Created fence request" << std::endl;

    if (!send_protobuf_message(request, response, MessageType::DB_FENCE)) {
        std::cout << "RPC failed[-]: " << "Failed to send message to lineairdb_service" << std::endl;
        return;
    }

    std::cout << "CLIENT: db_fence completed" << std::endl;
}

bool LineairDBClient::send_message(const std::string& serialized_request, std::string& serialized_response) {
    if (!connected_) {
        std::cout << "SEND_MESSAGE: Not connected!" << std::endl;
        return false;
    }
    
    std::cout << "SEND_MESSAGE: Sending message of size " << serialized_request.size() << " bytes" << std::endl;
    
    // send message size first (4 bytes)
    uint32_t message_size = htonl(serialized_request.size());
    std::cout << "SEND_MESSAGE: Sending size header: " << serialized_request.size() << " (network order: " << message_size << ")" << std::endl;
    
    ssize_t size_sent = send(socket_fd_, &message_size, sizeof(message_size), 0);
    if (size_sent != sizeof(message_size)) {
        std::cout << "SEND_MESSAGE: Failed to send size header, sent " << size_sent << " bytes instead of " << sizeof(message_size) << std::endl;
        return false;
    }
    std::cout << "SEND_MESSAGE: Size header sent successfully" << std::endl;

    // send message body
    std::cout << "SEND_MESSAGE: Sending message body..." << std::endl;
    ssize_t body_sent = send(socket_fd_, serialized_request.data(), serialized_request.size(), 0);
    if (body_sent != static_cast<ssize_t>(serialized_request.size())) {
        std::cout << "SEND_MESSAGE: Failed to send message body, sent " << body_sent << " bytes instead of " << serialized_request.size() << std::endl;
        return false;
    }
    std::cout << "SEND_MESSAGE: Message body sent successfully" << std::endl;

    // receive response size
    std::cout << "SEND_MESSAGE: Waiting for response size..." << std::endl;
    uint32_t response_size;
    ssize_t size_received = recv(socket_fd_, &response_size, sizeof(response_size), MSG_WAITALL);
    if (size_received != sizeof(response_size)) {
        std::cout << "SEND_MESSAGE: Failed to receive response size, got " << size_received << " bytes" << std::endl;
        return false;
    }
    response_size = ntohl(response_size);
    std::cout << "SEND_MESSAGE: Received response size: " << response_size << " bytes" << std::endl;

    // receive response body
    std::cout << "SEND_MESSAGE: Waiting for response body..." << std::endl;
    serialized_response.resize(response_size);
    ssize_t body_received = recv(socket_fd_, &serialized_response[0], response_size, MSG_WAITALL);
    if (body_received != static_cast<ssize_t>(response_size)) {
        std::cout << "SEND_MESSAGE: Failed to receive response body, got " << body_received << " bytes instead of " << response_size << std::endl;
        return false;
    }
    std::cout << "SEND_MESSAGE: Response body received successfully" << std::endl;
    
    return true;
}

template<typename RequestType, typename ResponseType>
bool LineairDBClient::send_protobuf_message(const RequestType& request, ResponseType& response, MessageType message_type) {
    std::cout << "PROTOBUF_MESSAGE: Starting protobuf message send" << std::endl;
    
    // serialize request
    std::string serialized_request = request.SerializeAsString();
    std::cout << "PROTOBUF_MESSAGE: Request serialized successfully, size: " << serialized_request.size() << " bytes" << std::endl;

    // send message with header
    std::string serialized_response;
    if (!send_message_with_header(serialized_request, serialized_response, message_type)) {
        std::cout << "PROTOBUF_MESSAGE: Failed to send message with header" << std::endl;
        return false;
    }

    // deserialize response
    if (!response.ParseFromString(serialized_response)) {
        std::cout << "PROTOBUF_MESSAGE: Failed to parse response" << std::endl;
        return false;
    }
    
    std::cout << "PROTOBUF_MESSAGE: Successfully completed protobuf message exchange" << std::endl;
    return true;
}

bool LineairDBClient::send_message_with_header(const std::string& serialized_request, std::string& serialized_response, MessageType message_type) {
    if (!connected_) {
        std::cout << "SEND_MESSAGE: Not connected!" << std::endl;
        return false;
    }
    
    std::cout << "SEND_MESSAGE: Sending message of size " << serialized_request.size() << " bytes with message_type " << static_cast<uint32_t>(message_type) << std::endl;

    // prepare message header
    MessageHeader header;
    header.sender_id = htobe64(1);  // TODO: replace with actual sender ID
    header.message_type = htonl(static_cast<uint32_t>(message_type));
    header.payload_size = htonl(static_cast<uint32_t>(serialized_request.size()));
    
    std::cout << "SEND_MESSAGE: Prepared header: sender_id=1, message_type=" << static_cast<uint32_t>(message_type) 
              << ", payload_size=" << serialized_request.size() << std::endl;
    
    // combine header and payload
    size_t total_size = sizeof(header) + serialized_request.size();
    std::vector<char> buffer(total_size);
    std::memcpy(buffer.data(), &header, sizeof(header));
    std::memcpy(buffer.data() + sizeof(header), serialized_request.c_str(), serialized_request.size());
    
    // send
    ssize_t bytes_sent = send(socket_fd_, buffer.data(), total_size, 0);
    if (bytes_sent != static_cast<ssize_t>(total_size)) {
        std::cout << "SEND_MESSAGE: Failed to send complete message, sent " << bytes_sent << " bytes instead of " << total_size << std::endl;
        return false;
    }
    
    std::cout << "SEND_MESSAGE: Successfully sent " << bytes_sent << " bytes" << std::endl;

    // receive response header
    MessageHeader response_header;
    ssize_t header_received = recv(socket_fd_, &response_header, sizeof(response_header), MSG_WAITALL);
    if (header_received != sizeof(response_header)) {
        std::cout << "SEND_MESSAGE: Failed to receive response header, received " << header_received << " bytes" << std::endl;
        return false;
    }

    // convert from network byte order to host byte order
    uint64_t response_sender_id = be64toh(response_header.sender_id);
    uint32_t response_message_type = ntohl(response_header.message_type);
    uint32_t response_payload_size = ntohl(response_header.payload_size);
    
    std::cout << "SEND_MESSAGE: Received response header: sender_id=" << response_sender_id 
              << ", message_type=" << response_message_type 
              << ", payload_size=" << response_payload_size << std::endl;

    // receive response payload
    if (response_payload_size > 0) {
        serialized_response.resize(response_payload_size);
        ssize_t payload_received = recv(socket_fd_, &serialized_response[0], response_payload_size, MSG_WAITALL);
        if (payload_received != static_cast<ssize_t>(response_payload_size)) {
            std::cout << "SEND_MESSAGE: Failed to receive response payload, received " << payload_received << " bytes instead of " << response_payload_size << std::endl;
            return false;
        }
        std::cout << "SEND_MESSAGE: Successfully received response payload (" << payload_received << " bytes)" << std::endl;
    } else {
        std::cout << "SEND_MESSAGE: No response payload (empty response)" << std::endl;
        serialized_response.clear();
    }
    
    std::cout << "SEND_MESSAGE: Message exchange completed successfully" << std::endl;
    return true;
}
