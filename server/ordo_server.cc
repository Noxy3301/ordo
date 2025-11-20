#include "ordo_server.hh"
#include "../common/log.h"
#include "lineairdb.pb.h"
#include "rpc/lineairdb_rpc.hh"

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
    LOG_INFO("Handling client connection fd=%d", client_socket);
    // Per-connection managers
    auto tx_manager = std::make_shared<TransactionManager>();
    auto rpc_handler = std::make_shared<LineairDBRpc>(db_manager_, tx_manager);

    while (true) {
        uint64_t sender_id;
        MessageType message_type;
        std::string payload;

        if (!MessageHandler::receive_message(client_socket, sender_id, message_type, payload)) {
            return;  // Client disconnected or error
        }

        std::string result;
        rpc_handler->handle_rpc(sender_id, message_type, payload, result);

        if (!MessageHandler::send_response(client_socket, 0, message_type, result)) {
            return;  // Failed to send response
        }
    }
}
