#ifndef LINEAIRDB_CLIENT_H
#define LINEAIRDB_CLIENT_H

#include <cstdint>
#include <string>
#include <utility>
#include <vector>
#include <memory>

#include "lineairdb.pb.h"

class LineairDBTransaction;
class BatchDispatcher;

struct KeyValue {
    std::string key;
    std::string value;
};

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
    TX_READ = 3,
    TX_WRITE = 4,
    TX_SCAN = 5,
    DB_FENCE = 6,
    DB_END_TRANSACTION = 7,
    TX_BATCH_OPERATIONS = 8
};

class LineairDBClient {
public:
    LineairDBClient(const std::string& host, int port);
    ~LineairDBClient();

    // connection management
    bool connect(const std::string& host, int port);
    void disconnect();
    bool is_connected() const;

    // transaction management
    int64_t tx_begin_transaction();
    void tx_abort(int64_t tx_id);

    // transaction operations
    std::string tx_read(LineairDBTransaction* tx, const std::string& key);
    bool tx_write(LineairDBTransaction* tx, const std::string& key, const std::string& value);
    std::vector<KeyValue> tx_scan(LineairDBTransaction* tx, const std::string& db_table_key, const std::string& first_key_part);

    // database operations: returns true if committed, false if aborted
    bool db_end_transaction(int64_t tx_id, bool isFence);
    void db_fence();

    // Global BatchDispatcher management (for cross-connection batching)
    static void set_batch_dispatcher(BatchDispatcher* dispatcher);
    static BatchDispatcher* get_batch_dispatcher();
    static void set_batching_enabled(bool enabled);
    static bool is_batching_enabled();

private:
    template<typename RequestType, typename ResponseType>
    bool send_protobuf_message(const RequestType& request, ResponseType& response, MessageType message_type);
    bool send_message(const std::string& serialized_request, std::string& serialized_response);
    bool send_message_with_header(const std::string& serialized_request, std::string& serialized_response, MessageType message_type);

    // Direct (non-batched) implementations
    std::string tx_read_direct(LineairDBTransaction* tx, const std::string& key);
    bool tx_write_direct(LineairDBTransaction* tx, const std::string& key, const std::string& value);
    std::vector<KeyValue> tx_scan_direct(LineairDBTransaction* tx, const std::string& db_table_key, const std::string& first_key_part);
    int64_t tx_begin_transaction_direct();
    bool db_end_transaction_direct(int64_t tx_id, bool isFence);
    void tx_abort_direct(int64_t tx_id);

    int socket_fd_;
    bool connected_;
    std::string host_;
    int port_;

    // Global batch dispatcher (shared across all clients)
    static BatchDispatcher* batch_dispatcher_;
    static bool batching_enabled_;
};

#endif // LINEAIRDB_CLIENT_H
