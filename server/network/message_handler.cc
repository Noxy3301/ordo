#include "message_handler.hh"
#include "../../common/log.h"

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
            LOG_ERROR("Failed to peek message header");
        } else {
            LOG_DEBUG("Client disconnected");
        }
        return false;
    }

    // If message header is not complete, wait for more data
    if (header_read < static_cast<ssize_t>(sizeof(header))) {
        LOG_WARNING("Incomplete message header (%zd/%zu bytes)", header_read, sizeof(header));
        return false;
    }

    // Convert message header (network order -> host order)
    sender_id = be64toh(header.sender_id);
    message_type = static_cast<MessageType>(ntohl(header.message_type));
    uint32_t payload_size = ntohl(header.payload_size);

    LOG_DEBUG("Received header: sender_id=%lu, message_type=%u, payload_size=%u", sender_id, static_cast<uint32_t>(message_type), payload_size);

    // Prepare buffer
    size_t total_size = sizeof(header) + payload_size;
    std::vector<char> buffer(total_size);

    // Read message header and payload
    ssize_t total_read = 0;
    while (total_read < total_size) {
        ssize_t bytes_read = recv(socket, buffer.data() + total_read, total_size - total_read, 0);

        if (bytes_read <= 0) {
            if (bytes_read < 0) {
                LOG_ERROR("Failed to receive data");
            } else {
                LOG_DEBUG("Client disconnected during message read");
            }
            return false;
        }

        total_read += bytes_read;
        if (total_read < total_size) {
            LOG_WARNING("Partial message received (%zu/%zu bytes)", total_read, total_size);
        }
    }

    LOG_DEBUG("Received complete message (%zu bytes)", total_size);
    
    // Extract payload
    payload = std::string(buffer.data() + sizeof(header), payload_size);
    return true;
}

bool MessageHandler::send_response(int socket, uint64_t sender_id, 
                                  MessageType message_type, const std::string& payload) {
    LOG_DEBUG("Sending response (%zu bytes)", payload.size());
    
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
        LOG_ERROR("Failed to send response");
        return false;
    }
    
    LOG_DEBUG("Response sent successfully");
    return true;
}
