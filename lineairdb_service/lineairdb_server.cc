#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include <cstring>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cassert>

#include "lineairdb_server.hh"
#include "lineairdb.pb.h"

LineairDBServer::LineairDBServer() :
    next_tx_id_(1) {
    // Initialize lineairdb
    LineairDB::Config conf;
    conf.enable_checkpointing = false;
    conf.enable_recovery      = false;
    conf.max_thread           = 1;  // TODO: multi threading?
    database_ = std::make_shared<LineairDB::Database>(conf);
    std::cout << "Server initialized with database config" << std::endl;
}

LineairDB::Transaction* LineairDBServer::get_transaction(int64_t tx_id) {
    auto it = transactions_.find(tx_id);
    if (it == transactions_.end()) {
        std::cout << "Transaction not found: " << tx_id << std::endl;
        return nullptr;
    }
    return it->second;
}

bool LineairDBServer::key_prefix_is_matching(const std::string& key_prefix, const std::string& key) {
    if (key.substr(0, key_prefix.size()) != key_prefix) return false;
    return true;
}

void LineairDBServer::run() {
    std::cout << "Starting LineairDB service on port 9999" << std::endl;
    
    // Create socket
    int server_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (server_socket < 0) {
        std::cerr << "Failed to create socket" << std::endl;
        return;
    }
    
    // Set SO_REUSEADDR option
    int reuse = 1;
    if (setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
        std::cerr << "Failed to set SO_REUSEADDR" << std::endl;
        close(server_socket);
        return;
    }

    struct sockaddr_in server_addr;
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(9999);

    // Bind socket
    if (bind(server_socket, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        std::cerr << "Failed to bind socket" << std::endl;
        close(server_socket);
        return;
    }

    // Listen for connections
    if (listen(server_socket, 5) < 0) {
        std::cerr << "Failed to listen on socket" << std::endl;
        close(server_socket);
        return;
    }

    std::cout << "LineairDB service listening on port 9999" << std::endl;

    while (true) {
        struct sockaddr_in client_addr;
        socklen_t client_addr_len = sizeof(client_addr);
        
        int client_socket = accept(server_socket, (struct sockaddr*)&client_addr, &client_addr_len);
        if (client_socket < 0) {
            std::cerr << "Failed to accept client connection" << std::endl;
            continue;
        }

        std::cout << "Client connected from " << inet_ntoa(client_addr.sin_addr) << std::endl;
        handle_client(client_socket);
        close(client_socket);
        std::cout << "Client disconnected" << std::endl;
    }

    close(server_socket);
}

void LineairDBServer::handle_client(int client_socket) {
    std::cout << "Handling client connection..." << std::endl;
    
    while (true) {
        // Peek message header (similar to Raft implementation)
        MessageHeader header;
        ssize_t header_read = recv(client_socket, &header, sizeof(header), MSG_PEEK);
        if (header_read <= 0) {
            if (header_read < 0) {
                std::cout << "Failed to peek message header" << std::endl;
            } else {
                std::cout << "Client disconnected" << std::endl;
            }
            return;
        }

        // if message header is not complete, wait for more data
        if (header_read < static_cast<ssize_t>(sizeof(header))) {
            std::cout << "Incomplete message header (" << header_read << "/" << sizeof(header) << " bytes)" << std::endl;
            continue;
        }

        // Convert message header (network order -> host order)
        uint64_t sender_id = be64toh(header.sender_id);
        MessageType message_type = static_cast<MessageType>(ntohl(header.message_type));
        uint32_t payload_size = ntohl(header.payload_size);

        std::cout << "Received header: sender_id=" << sender_id 
                  << ", message_type=" << static_cast<uint32_t>(message_type)
                  << ", payload_size=" << payload_size << std::endl;

        // Prepare buffer
        size_t total_size = sizeof(header) + payload_size;
        std::vector<char> buffer(total_size);

        // Read message header and payload
        ssize_t total_read = 0;
        while (total_read < total_size) {
            ssize_t bytes_read = recv(client_socket, buffer.data() + total_read, total_size - total_read, 0);

            if (bytes_read <= 0) {
                if (bytes_read < 0) {
                    std::cout << "Failed to receive data" << std::endl;
                } else {
                    std::cout << "Client disconnected during message read" << std::endl;
                }
                return;
            }

            total_read += bytes_read;
            if (total_read < total_size) {
                std::cout << "Partial message received (" << total_read << "/" << total_size << " bytes)" << std::endl;
            }
        }

        std::cout << "Received complete message (" << total_size << " bytes)" << std::endl;

        // Handle RPC
        std::string payload(buffer.data() + sizeof(header), payload_size);
        std::string result = "";
        handleRPC(sender_id, message_type, payload, result);
        
        // Send response (including empty responses)
        std::cout << "Sending response (" << result.size() << " bytes)" << std::endl;
        
        // Prepare response header
        MessageHeader response_header;
        response_header.sender_id = htobe64(0);  // server ID
        response_header.message_type = htonl(static_cast<uint32_t>(message_type));  // echo back message type
        response_header.payload_size = htonl(static_cast<uint32_t>(result.size()));

        // Combine header and response
        size_t response_total_size = sizeof(response_header) + result.size();
        std::vector<char> response_buffer(response_total_size);
        std::memcpy(response_buffer.data(), &response_header, sizeof(response_header));
        std::memcpy(response_buffer.data() + sizeof(response_header), result.c_str(), result.size());

        // Send response
        ssize_t bytes_sent = send(client_socket, response_buffer.data(), response_total_size, 0);
        if (bytes_sent != static_cast<ssize_t>(response_total_size)) {
            std::cout << "Failed to send response" << std::endl;
            return;
        }
        
        std::cout << "Response sent successfully" << std::endl;
    }
}

void LineairDBServer::handleRPC(uint64_t sender_id, MessageType message_type, const std::string& message, std::string& result) {
    std::cout << "Handling RPC: message_type=" << static_cast<uint32_t>(message_type) << std::endl;
    
    switch(message_type) {
        case MessageType::TX_BEGIN_TRANSACTION:
            handleTxBeginTransaction(message, result);
            return;
        case MessageType::TX_ABORT:
            handleTxAbort(message, result);
            return;
        case MessageType::TX_IS_ABORTED:
            handleTxIsAborted(message, result);
            return;
        case MessageType::TX_READ:
            handleTxRead(message, result);
            return;
        case MessageType::TX_WRITE:
            handleTxWrite(message, result);
            return;
        case MessageType::TX_SCAN:
            handleTxScan(message, result);
            return;
        case MessageType::DB_FENCE:
            handleDbFence(message, result);
            return;
        case MessageType::DB_END_TRANSACTION:
            handleDbEndTransaction(message, result);
            return;
        default:
            std::cout << "Unknown message type: " << static_cast<uint32_t>(message_type) << std::endl;
            return;
    }
}

void LineairDBServer::handleTxBeginTransaction(const std::string& message, std::string& result) {
    std::cout << "Handling TxBeginTransaction" << std::endl;
    
    LineairDB::Protocol::TxBeginTransaction::Request request;
    LineairDB::Protocol::TxBeginTransaction::Response response;
    
    request.ParseFromString(message);
    
    // Start new transaction
    auto& tx = database_->BeginTransaction();
    int64_t tx_id = generate_tx_id();
    transactions_[tx_id] = &tx;
    
    response.set_transaction_id(tx_id);
    result = response.SerializeAsString();
    
    std::cout << "Created transaction: " << tx_id << std::endl;
}

void LineairDBServer::handleTxAbort(const std::string& message, std::string& result) {
    std::cout << "Handling TxAbort" << std::endl;
    
    LineairDB::Protocol::TxAbort::Request request;
    LineairDB::Protocol::TxAbort::Response response;
    
    request.ParseFromString(message);
    
    int64_t tx_id = request.transaction_id();
    auto* tx = get_transaction(tx_id);
    if (tx) {
        tx->Abort();
        std::cout << "Aborted transaction: " << tx_id << std::endl;
    } else {
        std::cout << "Transaction not found for abort: " << tx_id << std::endl;
    }
    
    result = response.SerializeAsString();
}

void LineairDBServer::handleTxIsAborted(const std::string& message, std::string& result) {
    std::cout << "Handling TxIsAborted" << std::endl;
    
    LineairDB::Protocol::TxIsAborted::Request request;
    LineairDB::Protocol::TxIsAborted::Response response;
    
    std::cout << "DEBUG: Parsing request from string of size: " << message.size() << std::endl;
    request.ParseFromString(message);
    
    int64_t tx_id = request.transaction_id();
    std::cout << "DEBUG: Extracted transaction_id: " << tx_id << std::endl;
    
    auto* tx = get_transaction(tx_id);
    if (tx) {
        bool is_aborted = tx->IsAborted();
        response.set_is_aborted(is_aborted);
        std::cout << "Transaction " << tx_id << " aborted status: " << is_aborted << std::endl;
    } else {
        response.set_is_aborted(true);  // not found = aborted
        std::cout << "Transaction not found, considering as aborted: " << tx_id << std::endl;
    }
    
    result = response.SerializeAsString();
    std::cout << "DEBUG: Serialized response, size: " << result.size() << std::endl;
    std::cout << "DEBUG: Response is_aborted value: " << response.is_aborted() << std::endl;
}

void LineairDBServer::handleTxRead(const std::string& message, std::string& result) {
    std::cout << "Handling TxRead" << std::endl;
    
    LineairDB::Protocol::TxRead::Request request;
    LineairDB::Protocol::TxRead::Response response;
    
    request.ParseFromString(message);
    
    int64_t tx_id = request.transaction_id();
    auto* tx = get_transaction(tx_id);
    if (tx) {
        auto read_result = tx->Read(request.key());
        if (read_result.first != nullptr) {
            response.set_found(true);
            std::string value(reinterpret_cast<const char*>(read_result.first), read_result.second);
            response.set_value(value);
        } else {
            response.set_found(false);
        }
        std::cout << "Read key '" << request.key() << "' from transaction " << tx_id << ": " << (read_result.first != nullptr ? "found" : "not found") << std::endl;
    } else {
        response.set_found(false);
        std::cout << "Transaction not found for read: " << tx_id << std::endl;
    }
    
    result = response.SerializeAsString();
}

void LineairDBServer::handleTxWrite(const std::string& message, std::string& result) {
    std::cout << "Handling TxWrite" << std::endl;
    
    LineairDB::Protocol::TxWrite::Request request;
    LineairDB::Protocol::TxWrite::Response response;
    
    request.ParseFromString(message);
    
    int64_t tx_id = request.transaction_id();
    auto* tx = get_transaction(tx_id);
    if (tx) {
        const std::string& value_str = request.value();
        tx->Write(request.key(), reinterpret_cast<const std::byte*>(value_str.c_str()), value_str.size());
        response.set_success(true);
        std::cout << "Wrote key '" << request.key() << "' to transaction " << tx_id << std::endl;
    } else {
        response.set_success(false);
        std::cout << "Transaction not found for write: " << tx_id << std::endl;
    }
    
    result = response.SerializeAsString();
}

void LineairDBServer::handleTxScan(const std::string& message, std::string& result) {
    std::cout << "Handling TxScan" << std::endl;
    
    LineairDB::Protocol::TxScan::Request request;
    LineairDB::Protocol::TxScan::Response response;
    
    request.ParseFromString(message);
    
    int64_t tx_id = request.transaction_id();
    auto* tx = get_transaction(tx_id);
    if (tx) {
        std::vector<std::string> keys;
        std::string table_prefix = request.db_table_key();
        std::string key_prefix = table_prefix + request.first_key_part();
        
        std::cout << "DEBUG SCAN: tx_id=" << tx_id << ", table_prefix='" << table_prefix 
                  << "', first_key_part='" << request.first_key_part() << "', key_prefix='" << key_prefix << "'" << std::endl;

        tx->Scan("", std::nullopt, [&keys, &key_prefix, &table_prefix, this](const std::string_view key, const std::pair<const void*, const size_t>& value) {
            std::string key_str(key);
            std::cout << "DEBUG SCAN CALLBACK: checking key='" << key_str << "' against prefix='" << key_prefix << "'" << std::endl;

            if (key_prefix_is_matching(key_prefix, key_str)) {
                std::string relative_key = key_str.substr(table_prefix.size());
                std::cout << "DEBUG SCAN CALLBACK: key matches, adding relative_key='" << relative_key << "'" << std::endl;
                keys.push_back(relative_key);
            } else {
                std::cout << "DEBUG SCAN CALLBACK: key does not match prefix, skipping" << std::endl;
            }
            return false;
        });
        
        std::cout << "DEBUG SCAN: completed scan, found " << keys.size() << " keys:" << std::endl;
        for (size_t i = 0; i < keys.size(); i++) {
            std::cout << "  [" << i << "] " << keys[i] << std::endl;
        }
        
        for (const auto& key : keys) {
            response.add_keys(key);
        }
        std::cout << "Scanned transaction " << tx_id << ", found " << keys.size() << " keys" << std::endl;
    } else {
        std::cout << "Transaction not found for scan: " << tx_id << std::endl;
    }
    
    result = response.SerializeAsString();
}

void LineairDBServer::handleDbFence(const std::string& message, std::string& result) {
    std::cout << "Handling DbFence" << std::endl;
    
    LineairDB::Protocol::DbFence::Request request;
    LineairDB::Protocol::DbFence::Response response;
    
    request.ParseFromString(message);
    
    database_->Fence();
    std::cout << "Database fence completed" << std::endl;
    
    result = response.SerializeAsString();
}

void LineairDBServer::handleDbEndTransaction(const std::string& message, std::string& result) {
    std::cout << "Handling DbEndTransaction" << std::endl;
    
    LineairDB::Protocol::DbEndTransaction::Request request;
    LineairDB::Protocol::DbEndTransaction::Response response;
    
    request.ParseFromString(message);
    
    int64_t tx_id = request.transaction_id();
    auto* tx = get_transaction(tx_id);
    if (tx) {
        bool fence = request.fence();
        database_->EndTransaction(*tx, [fence, tx_id](LineairDB::TxStatus status) {
            std::cout << "Transaction " << tx_id << " ended with status: " << static_cast<int>(status) << ", fence=" << fence << std::endl;
        });
        transactions_.erase(tx_id);  // remove from map
        std::cout << "Ended transaction " << tx_id << " with fence=" << fence << std::endl;
    } else {
        std::cout << "Transaction not found for end: " << tx_id << std::endl;
    }
    
    result = response.SerializeAsString();
}

int main(int argc, char** argv) {
    std::cout << "Starting LineairDB server..." << std::endl;
    LineairDBServer server;
    server.run();  // Start listening
    return 0;
}