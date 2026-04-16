#include "lineairdb_server.hh"
#include "../common/log.h"

LineairDBServer::LineairDBServer() : TcpServer(9999) {}

void LineairDBServer::init() {
    if (!db_manager_) {
        db_manager_ = std::make_shared<DatabaseManager>();
    }
    LOG_INFO("LineairDB server initialized successfully");
}

std::unique_ptr<ConnectionContext> LineairDBServer::create_connection(int fd, int group_id) {
    auto tx_manager = std::make_shared<TransactionManager>();
    auto rpc_handler = std::make_shared<LineairDBRpc>(db_manager_, tx_manager, row_counts_);
    return std::make_unique<ConnectionContext>(
        ConnectionContext{fd, group_id, std::move(tx_manager), std::move(rpc_handler)});
}

std::shared_ptr<DatabaseManager> LineairDBServer::get_db_manager() {
    return db_manager_;
}
