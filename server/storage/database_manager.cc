#include "database_manager.hh"
#include "../../common/log.h"

#include <iostream>

DatabaseManager::DatabaseManager() {
    // Initialize lineairdb
    // TODO: make configurable
    LineairDB::Config conf;
    conf.enable_checkpointing = false;
    conf.enable_recovery      = false;
    conf.enable_logging       = false;  // avoid per-thread LineairDB logs
    conf.max_thread           = 1;
    conf.concurrency_control_protocol = LineairDB::Config::ConcurrencyControl::Silo;
    database_ = std::make_shared<LineairDB::Database>(conf);
    LOG_INFO("Database manager initialized");
}
