#include "tcp_server.hh"
#include "../../common/log.h"

#include <iostream>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <thread>

TcpServer::TcpServer(uint16_t port) : port_(port) {}

void TcpServer::run() {
    LOG_INFO("Starting server on port %d", port_);
    
    int server_socket;
    if (!setup_and_listen(server_socket)) {
        return;
    }
    
    LOG_INFO("Server listening on port %d", port_);
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
    if (listen(server_socket, 128) < 0) {
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

        // Hand off each client to a dedicated thread
        auto client_ip = std::string(inet_ntoa(client_addr.sin_addr));
        LOG_DEBUG("Client connected from %s", client_ip.c_str());

        std::thread([this, client_socket, client_ip]() {
            // Process the client in this thread
            handle_client(client_socket);
            // Ensure socket is closed when done
            close(client_socket);
            LOG_DEBUG("Client disconnected (%s)", client_ip.c_str());
        }).detach();
    }
}
