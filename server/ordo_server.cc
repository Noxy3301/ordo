#include "ordo_server.hh"
#include "../common/log.h"
#include "lineairdb.pb.h"

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
    WorkerManager worker_manager(db_manager_, tx_manager);

    while (true) {
        uint64_t sender_id;
        MessageType message_type;
        std::string payload;

        if (!MessageHandler::receive_message(client_socket, sender_id, message_type, payload)) {
            return;  // Client disconnected or error
        }

        std::string result;

        if (message_type == MessageType::TX_BEGIN_TRANSACTION) {
            int64_t tx_id = -1;
            result = worker_manager.begin_on_new_worker(sender_id, payload, tx_id);
        } else {
            // Extract tx_id from payload to route to the correct worker
            // TODO: Add a 64-bit tx_id header to eliminate double parsing
            int64_t tx_id = -1;
            switch (message_type) {
                case MessageType::TX_ABORT: {
                    LineairDB::Protocol::TxAbort::Request request; 
                    if (!request.ParseFromString(payload)) {
                        LOG_WARNING("Failed to parse TxAbort request");
                    }
                    tx_id = request.transaction_id();
                    break;
                }
                case MessageType::TX_IS_ABORTED: {
                    LineairDB::Protocol::TxIsAborted::Request request; 
                    if (!request.ParseFromString(payload)) {
                        LOG_WARNING("Failed to parse TxIsAborted request");
                    }
                    tx_id = request.transaction_id();
                    break;
                }
                case MessageType::TX_READ: {
                    LineairDB::Protocol::TxRead::Request request; 
                    if (!request.ParseFromString(payload)) {
                        LOG_WARNING("Failed to parse TxRead request");
                    }
                    tx_id = request.transaction_id();
                    break;
                }
                case MessageType::TX_WRITE: {
                    LineairDB::Protocol::TxWrite::Request request; 
                    if (!request.ParseFromString(payload)) {
                        LOG_WARNING("Failed to parse TxWrite request");
                    }
                    tx_id = request.transaction_id();
                    break;
                }
                case MessageType::TX_SCAN: {
                    LineairDB::Protocol::TxScan::Request request; 
                    if (!request.ParseFromString(payload)) {
                        LOG_WARNING("Failed to parse TxScan request");
                    }
                    tx_id = request.transaction_id();
                    break;
                }
                case MessageType::DB_END_TRANSACTION: {
                    LineairDB::Protocol::DbEndTransaction::Request request; 
                    if (!request.ParseFromString(payload)) {
                        LOG_WARNING("Failed to parse DbEndTransaction request");
                    }
                    tx_id = request.transaction_id();
                    break;
                }
                case MessageType::DB_FENCE: {
                    // Transaction-independent control operation
                    // For now, handle it with a one-off worker to reuse the existing RPC path
                    // TODO: Add a shared worker dedicated to such control operations
                    TransactionWorker temp(db_manager_, tx_manager);
                    result = temp.enqueue_and_wait(sender_id, message_type, payload);
                    // Send response and continue to next loop
                    if (!MessageHandler::send_response(client_socket, 0, message_type, result)) {
                        return;
                    }
                    continue;
                }
                default: {
                    LOG_ERROR("Unknown message type: %u", static_cast<uint32_t>(message_type));
                    result.clear();
                    break;
                }
            }

            if (tx_id <= 0) {
                LOG_WARNING("Invalid or missing tx_id for message_type=%u", static_cast<uint32_t>(message_type));
                result.clear();
            } else {
                result = worker_manager.dispatch_to_worker(sender_id, tx_id, message_type, payload);
            }
        }

        if (!MessageHandler::send_response(client_socket, 0, message_type, result)) {
            return;  // Failed to send response
        }
    }
}
