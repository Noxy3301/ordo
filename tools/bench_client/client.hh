#pragma once

#include "protocol/message.hh"

#include <google/protobuf/message_lite.h>

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

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

    // Flat-binary scan response decoder. The server emits its own framing for
    // `TX_GET_MATCHING_KEYS_AND_VALUES_{IN_RANGE,FROM_PREFIX}` (not protobuf):
    //   [1B is_aborted][4B klen][key][4B vlen][value] ×N [4B sentinel=0]
    // Returns the pairs through `out_kvs` (moved out of recv_buf_). `is_aborted`
    // is set from the leading byte.
    bool call_flat_scan(MessageType type,
                        const google::protobuf::MessageLite& req,
                        bool* is_aborted,
                        std::vector<std::pair<std::string, std::string>>* out_kvs);

private:
    int fd_ = -1;
    std::string send_buf_;  // reused across calls to avoid alloc
    std::string recv_buf_;  // reused across calls
};

}  // namespace bench_client
