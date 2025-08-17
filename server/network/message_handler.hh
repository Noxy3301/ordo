#pragma once

#include <string>

#include "../protocol/message.hh"

class MessageHandler {
public:
    static bool receive_message(int socket, uint64_t& sender_id, 
                               MessageType& message_type, std::string& payload);
    static bool send_response(int socket, uint64_t sender_id, 
                             MessageType message_type, const std::string& payload);
};
