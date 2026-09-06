#pragma once

#include <memory>

#include "lineairdb/lineairdb.h"

class DatabaseManager {
public:
    DatabaseManager();
    ~DatabaseManager() = default;

    std::shared_ptr<LineairDB::Database> get_database() const { return database_; }
    
private:
    std::shared_ptr<LineairDB::Database> database_;
};
