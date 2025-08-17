#ifndef LINEAIRDB_CLIENT_H
#define LINEAIRDB_CLIENT_H

#include <cstdint>
#include <string>
#include <utility>
#include <vector>
#include <memory>

#include "lineairdb.pb.h"

// Message header for RPC communication (matching server implementation)
struct MessageHeader {
    uint64_t sender_id;      // client ID
    uint32_t message_type;   // OpCode from protobuf
    uint32_t payload_size;   // size of the protobuf payload
};

// MessageType enum (corresponds to protobuf OpCode)
enum class MessageType : uint32_t {
    UNKNOWN = 0,
    TX_BEGIN_TRANSACTION = 1,
    TX_ABORT = 2,
    TX_IS_ABORTED = 3,
    TX_READ = 4,
    TX_WRITE = 5,
    TX_SCAN = 6,
    DB_FENCE = 7,
    DB_END_TRANSACTION = 8
};

class LineairDBClient {
public:
    LineairDBClient();
    ~LineairDBClient();

    // connection management
    bool connect(const std::string& host, int port);
    void disconnect();
    bool is_connected() const;

    // transaction management
    int64_t tx_begin_transaction();
    void tx_abort(int64_t tx_id);
    bool tx_is_aborted(int64_t tx_id);

    // transaction operations
    std::string tx_read(int64_t tx_id, const std::string& key);
    bool tx_write(int64_t tx_id, const std::string& key, const std::string& value);
    std::vector<std::string> tx_scan(int64_t tx_id, const std::string& db_table_key, const std::string& first_key_part);

    // database operations
    void db_end_transaction(int64_t tx_id, bool isFence);
    void db_fence();

private:
    template<typename RequestType, typename ResponseType>
    bool send_protobuf_message(const RequestType& request, ResponseType& response, MessageType message_type);
    bool send_message(const std::string& serialized_request, std::string& serialized_response);
    bool send_message_with_header(const std::string& serialized_request, std::string& serialized_response, MessageType message_type);

    int socket_fd_;
    bool connected_;
    std::string host_;
    int port_;
};

#endif // LINEAIRDB_CLIENT_H
