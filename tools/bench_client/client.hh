#pragma once

#include "protocol/message.hh"

#include <google/protobuf/message_lite.h>

#include <cstdint>
#include <string>

namespace bench_client {

// Single TCP connection to lineairdb-server. Not thread-safe; one instance per worker thread.
class Client {
public:
    Client();
    ~Client();

    Client(const Client&) = delete;
    Client& operator=(const Client&) = delete;

    bool connect(const std::string& host, int port);
    void disconnect();
    bool is_connected() const { return fd_ >= 0; }

    // Serialize `req`, send with header type `type`, receive + parse into `resp`.
    // Returns false on any wire/parse error. No retry, no reconnect.
    bool call(MessageType type, const google::protobuf::MessageLite& req,
              google::protobuf::MessageLite& resp);

private:
    int fd_ = -1;
    std::string send_buf_;  // reused across calls to avoid alloc
    std::string recv_buf_;  // reused across calls
};

}  // namespace bench_client
