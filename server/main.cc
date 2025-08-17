#include <iostream>

#include "ordo_server.hh"

int main(int argc, char** argv) {
    std::cout << "Starting Ordo server..." << std::endl;
    
    OrdoServer server;
    server.init();
    server.run();  // Start listening
    
    return 0;
}