#include "message_handler.hh"

#include <iostream>
#include <vector>
#include <cstring>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

bool MessageHandler::receive_message(int socket, uint64_t& sender_id, 
                                    MessageType& message_type, std::string& payload) {
    // Peek message header
    MessageHeader header;
    ssize_t header_read = recv(socket, &header, sizeof(header), MSG_PEEK);
    if (header_read <= 0) {
        if (header_read < 0) {
            std::cout << "Failed to peek message header" << std::endl;
        } else {
            std::cout << "Client disconnected" << std::endl;
        }
        return false;
    }

    // If message header is not complete, wait for more data
    if (header_read < static_cast<ssize_t>(sizeof(header))) {
        std::cout << "Incomplete message header (" << header_read << "/" << sizeof(header) << " bytes)" << std::endl;
        return false;
    }

    // Convert message header (network order -> host order)
    sender_id = be64toh(header.sender_id);
    message_type = static_cast<MessageType>(ntohl(header.message_type));
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
        ssize_t bytes_read = recv(socket, buffer.data() + total_read, total_size - total_read, 0);

        if (bytes_read <= 0) {
            if (bytes_read < 0) {
                std::cout << "Failed to receive data" << std::endl;
            } else {
                std::cout << "Client disconnected during message read" << std::endl;
            }
            return false;
        }

        total_read += bytes_read;
        if (total_read < total_size) {
            std::cout << "Partial message received (" << total_read << "/" << total_size << " bytes)" << std::endl;
        }
    }

    std::cout << "Received complete message (" << total_size << " bytes)" << std::endl;
    
    // Extract payload
    payload = std::string(buffer.data() + sizeof(header), payload_size);
    return true;
}

bool MessageHandler::send_response(int socket, uint64_t sender_id, 
                                  MessageType message_type, const std::string& payload) {
    std::cout << "Sending response (" << payload.size() << " bytes)" << std::endl;
    
    // Prepare response header
    MessageHeader response_header;
    response_header.sender_id = htobe64(sender_id);
    response_header.message_type = htonl(static_cast<uint32_t>(message_type));
    response_header.payload_size = htonl(static_cast<uint32_t>(payload.size()));

    // Combine header and response
    size_t response_total_size = sizeof(response_header) + payload.size();
    std::vector<char> response_buffer(response_total_size);
    std::memcpy(response_buffer.data(), &response_header, sizeof(response_header));
    std::memcpy(response_buffer.data() + sizeof(response_header), payload.c_str(), payload.size());

    // Send response
    ssize_t bytes_sent = send(socket, response_buffer.data(), response_total_size, 0);
    if (bytes_sent != static_cast<ssize_t>(response_total_size)) {
        std::cout << "Failed to send response" << std::endl;
        return false;
    }
    
    std::cout << "Response sent successfully" << std::endl;
    return true;
}
