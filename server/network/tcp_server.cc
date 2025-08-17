#include "tcp_server.hh"

#include <iostream>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

TcpServer::TcpServer(uint16_t port) : port_(port) {}

void TcpServer::run() {
    std::cout << "Starting server on port " << port_ << std::endl;
    
    int server_socket;
    if (!setup_and_listen(server_socket)) {
        return;
    }
    
    std::cout << "Server listening on port " << port_ << std::endl;
    accept_clients(server_socket);
    close(server_socket);
}

bool TcpServer::setup_and_listen(int& server_socket) {
    // Create socket
    server_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (server_socket < 0) {
        std::cerr << "Failed to create socket" << std::endl;
        return false;
    }
    
    // Set SO_REUSEADDR option
    int reuse = 1;
    if (setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
        std::cerr << "Failed to set SO_REUSEADDR" << std::endl;
        close(server_socket);
        return false;
    }

    struct sockaddr_in server_addr;
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(port_);

    // Bind socket
    if (bind(server_socket, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        std::cerr << "Failed to bind socket" << std::endl;
        close(server_socket);
        return false;
    }

    // Listen for connections
    if (listen(server_socket, 5) < 0) {
        std::cerr << "Failed to listen on socket" << std::endl;
        close(server_socket);
        return false;
    }
    
    return true;
}

void TcpServer::accept_clients(int server_socket) {
    while (true) {
        struct sockaddr_in client_addr;
        socklen_t client_addr_len = sizeof(client_addr);
        
        int client_socket = accept(server_socket, (struct sockaddr*)&client_addr, &client_addr_len);
        if (client_socket < 0) {
            std::cerr << "Failed to accept client connection" << std::endl;
            continue;
        }

        std::cout << "Client connected from " << inet_ntoa(client_addr.sin_addr) << std::endl;
        handle_client(client_socket);
        close(client_socket);
        std::cout << "Client disconnected" << std::endl;
    }
}
