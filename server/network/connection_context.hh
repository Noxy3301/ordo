#pragma once

#include <memory>

#include "../rpc/lineairdb_rpc.hh"
#include "../storage/transaction_manager.hh"

struct ConnectionContext {
    int fd;
    int group_id;
    std::shared_ptr<TransactionManager> tx_manager;
    std::shared_ptr<LineairDBRpc> rpc_handler;
};
