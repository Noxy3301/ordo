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
    // Read fixed-size header
    MessageHeader net_header{};
    ssize_t header_read = recv(socket, &net_header, sizeof(net_header), MSG_WAITALL);
    if (header_read != static_cast<ssize_t>(sizeof(net_header))) {
        if (header_read < 0) {
            LOG_ERROR("Failed to receive message header");
        } else {
            LOG_DEBUG("Client disconnected while receiving header");
        }
        return false;
    }

    // Convert message header (network order -> host order)
    sender_id = be64toh(net_header.sender_id);
    message_type = static_cast<MessageType>(ntohl(net_header.message_type));
    uint32_t payload_size = ntohl(net_header.payload_size);

    LOG_DEBUG("Received header: sender_id=%lu, message_type=%u, payload_size=%u", 
              sender_id, static_cast<uint32_t>(message_type), payload_size);

    // Read payload (if exists)
    payload.clear();
    if (payload_size > 0) {
        payload.resize(payload_size);
        ssize_t body_read = recv(socket, &payload[0], payload_size, MSG_WAITALL);
        if (body_read != static_cast<ssize_t>(payload_size)) {
            if (body_read < 0) {
                LOG_ERROR("Failed to receive message payload");
            } else {
                LOG_DEBUG("Client disconnected while receiving payload");
            }
            return false;
        }
    }

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
