#include <iostream>

#include "lineairdb_server.hh"
#include "../common/log.h"

int main(int argc, char** argv) {
    LOG_INFO("Starting LineairDB server...");
    
    LineairDBServer server;
    server.init();
    server.run();  // Start listening
    
    return 0;
}