#include "ordo_server.hh"
#include "../common/log.h"

#include <iostream>

OrdoServer::OrdoServer() : TcpServer(9999) {}

void OrdoServer::init() {
    // Initialize components in dependency order
    if (!db_manager_) {
        db_manager_ = std::make_shared<DatabaseManager>();
    }

    LOG_INFO("Ordo server initialized successfully");
}

void OrdoServer::handle_client(int client_socket) {
    LOG_DEBUG("Handling client connection...");
    // Per-connection managers (no cross-thread sharing -> no mutex needed)
    auto tx_manager = std::make_shared<TransactionManager>();
    auto rpc_handler = std::make_shared<LineairDBRpc>(db_manager_, tx_manager);
    
    while (true) {
        uint64_t sender_id;
        MessageType message_type;
        std::string payload;
        
        // Receive message
        if (!MessageHandler::receive_message(client_socket, sender_id, message_type, payload)) {
            return;  // Client disconnected or error
        }
        
        // Handle RPC using this connection's handler
        std::string result = "";
        rpc_handler->handle_rpc(sender_id, message_type, payload, result);
        
        // Send response
        if (!MessageHandler::send_response(client_socket, 0, message_type, result)) {
            return;  // Failed to send response
        }
    }
}
