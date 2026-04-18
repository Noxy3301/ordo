#include "lineairdb_server.hh"
#include "../common/log.h"
#include "lineairdb.pb.h"
#include "rpc/lineairdb_rpc.hh"

#include <iostream>

LineairDBServer::LineairDBServer() : TcpServer(9999) {}

void LineairDBServer::init() {
    // Initialize components in dependency order
    if (!db_manager_) {
        db_manager_ = std::make_shared<DatabaseManager>();
    }

    LOG_INFO("LineairDB server initialized successfully");
}

void LineairDBServer::handle_client(int client_socket) {
    LOG_INFO("Handling client connection fd=%d", client_socket);
    // Per-connection managers
    auto tx_manager = std::make_shared<TransactionManager>();
    auto rpc_handler = std::make_shared<LineairDBRpc>(db_manager_, tx_manager, row_counts_);

    // Reused across RPCs so the capacity grown by receive_message/SerializeAsString
    // survives instead of being freed and re-allocated each iteration
    std::string payload;
    std::string result;

    while (true) {
        uint64_t sender_id;
        MessageType message_type;

        if (!MessageHandler::receive_message(client_socket, sender_id, message_type, payload)) {
            break;  // Client disconnected or error
        }

        rpc_handler->handle_rpc(sender_id, message_type, payload, result);

        if (!MessageHandler::send_response_writev(client_socket, 0, message_type, result)) {
            break;  // Failed to send response
        }
    }
}
