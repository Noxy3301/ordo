#include "database_manager.hh"

#include <iostream>

DatabaseManager::DatabaseManager() {
    // Initialize lineairdb
    // TODO: make configurable
    LineairDB::Config conf;
    conf.enable_checkpointing = false;
    conf.enable_recovery      = false;
    conf.max_thread           = 1;  // TODO: multi threading?
    database_ = std::make_shared<LineairDB::Database>(conf);
    std::cout << "Database manager initialized" << std::endl;
}
