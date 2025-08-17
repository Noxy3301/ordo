#include "ordo_server.hh"

#include <iostream>

OrdoServer::OrdoServer() : TcpServer(9999) {}

void OrdoServer::init() {
    // Initialize components in dependency order
    if (!db_manager_) {
        db_manager_ = std::make_shared<DatabaseManager>();
    }
    
    if (!tx_manager_) {
        tx_manager_ = std::make_shared<TransactionManager>();
    }
    
    if (!rpc_handler_) {
        rpc_handler_ = std::make_shared<LineairDBRpc>(db_manager_, tx_manager_);
    }
    
    std::cout << "Ordo server initialized successfully" << std::endl;
}

void OrdoServer::handle_client(int client_socket) {
    std::cout << "Handling client connection..." << std::endl;
    
    while (true) {
        uint64_t sender_id;
        MessageType message_type;
        std::string payload;
        
        // Receive message
        if (!MessageHandler::receive_message(client_socket, sender_id, message_type, payload)) {
            return;  // Client disconnected or error
        }
        
        // Handle RPC
        std::string result = "";
        rpc_handler_->handle_rpc(sender_id, message_type, payload, result);
        
        // Send response
        if (!MessageHandler::send_response(client_socket, 0, message_type, result)) {
            return;  // Failed to send response
        }
    }
}
