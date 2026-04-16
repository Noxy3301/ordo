#pragma once

#include <memory>

#include "network/tcp_server.hh"
#include "network/connection_context.hh"
#include "storage/database_manager.hh"
#include "rpc/lineairdb_rpc.hh"
#include "storage/transaction_manager.hh"

class LineairDBServer : public TcpServer {
public:
    LineairDBServer();
    ~LineairDBServer() = default;

    void init();

protected:
    std::unique_ptr<ConnectionContext> create_connection(int fd, int group_id) override;
    std::shared_ptr<DatabaseManager> get_db_manager() override;

private:
    std::shared_ptr<DatabaseManager> db_manager_;
    std::shared_ptr<TableRowCounts> row_counts_ = std::make_shared<TableRowCounts>();
};
