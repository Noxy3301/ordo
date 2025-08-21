#include <iostream>

#include "ordo_server.hh"
#include "../common/log.h"

int main(int argc, char** argv) {
    LOG_INFO("Starting Ordo server...");
    
    OrdoServer server;
    server.init();
    server.run();  // Start listening
    
    return 0;
}