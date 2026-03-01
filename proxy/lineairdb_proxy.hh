#ifndef LINEAIRDB_PROXY_H
#define LINEAIRDB_PROXY_H

#include <cstdint>
#include <string>
#include <utility>
#include <vector>
#include <memory>

#include "lineairdb.pb.h"

class LineairDBTransaction;

struct KeyValue {
    std::string key;
    std::string value;
};

struct SecondaryIndexEntry {
    std::string secondary_key;
    std::vector<std::string> primary_keys;
};

// Message header for RPC communication (matching server implementation)
struct MessageHeader {
    uint64_t sender_id;      // sender ID
    uint32_t message_type;   // OpCode from protobuf
    uint32_t payload_size;   // size of the protobuf payload
};

// MessageType enum (corresponds to protobuf OpCode)
enum class MessageType : uint32_t {
    UNKNOWN = 0,

    // Transaction lifecycle
    TX_BEGIN_TRANSACTION = 1,
    TX_ABORT = 2,

    // Primary key operations
    TX_READ = 3,
    TX_WRITE = 4,
    TX_DELETE = 5,

    // Secondary index operations
    TX_READ_SECONDARY_INDEX = 6,
    TX_WRITE_SECONDARY_INDEX = 7,
    TX_DELETE_SECONDARY_INDEX = 8,
    TX_UPDATE_SECONDARY_INDEX = 9,

    // Primary key scan operations
    TX_GET_MATCHING_KEYS_IN_RANGE = 10,
    TX_GET_MATCHING_KEYS_AND_VALUES_IN_RANGE = 11,
    TX_GET_MATCHING_KEYS_AND_VALUES_FROM_PREFIX = 12,
    TX_FETCH_LAST_KEY_IN_RANGE = 13,
    TX_FETCH_FIRST_KEY_WITH_PREFIX = 14,
    TX_FETCH_NEXT_KEY_WITH_PREFIX = 15,

    // Secondary index scan operations
    TX_GET_MATCHING_PRIMARY_KEYS_IN_RANGE = 16,
    TX_GET_MATCHING_PRIMARY_KEYS_FROM_PREFIX = 17,
    TX_FETCH_LAST_PRIMARY_KEY_IN_SECONDARY_RANGE = 18,
    TX_FETCH_LAST_SECONDARY_ENTRY_IN_RANGE = 19,

    // Database operations
    DB_FENCE = 20,
    DB_END_TRANSACTION = 21,
    DB_CREATE_TABLE = 22,
    DB_SET_TABLE = 23,
    DB_CREATE_SECONDARY_INDEX = 24
};

/**
 * RPC client that provides the same transactional API as LineairDB,
 * but internally forwards all operations to a remote server via RPC over TCP.
 *
 * In this disaggregated architecture, MySQL instances do not embed LineairDB
 * directly; instead, each THD holds a LineairDBProxy that maintains a
 * TCP connection to the remote LineairDB server. Managed via LineairDBThdCtx.
 */
class LineairDBProxy {
public:
    LineairDBProxy(const std::string& host, int port);
    ~LineairDBProxy();

    // connection management
    bool connect(const std::string& host, int port);
    void disconnect();
    bool is_connected() const;

    // transaction management
    int64_t tx_begin_transaction();
    void tx_abort(int64_t tx_id);

    // primary key operations
    std::string tx_read(LineairDBTransaction* tx, const std::string& key);
    bool tx_write(LineairDBTransaction* tx, const std::string& key, const std::string& value);
    bool tx_delete(LineairDBTransaction* tx, const std::string& key);
    std::vector<KeyValue> tx_scan(LineairDBTransaction* tx, const std::string& db_table_key, const std::string& first_key_part);
    std::vector<KeyValue> tx_scan_range(LineairDBTransaction* tx,
                                        const std::string& start_key,
                                        const std::string& end_key,
                                        const std::string& exclusive_end_key,
                                        bool reverse,
                                        bool include_values,
                                        uint32_t limit,
                                        const std::string& after_key);

    // secondary index operations
    std::vector<std::string> tx_read_secondary_index(LineairDBTransaction* tx,
                                                     const std::string& index_name,
                                                     const std::string& secondary_key);
    bool tx_write_secondary_index(LineairDBTransaction* tx,
                                  const std::string& index_name,
                                  const std::string& secondary_key,
                                  const std::string& primary_key);
    bool tx_delete_secondary_index(LineairDBTransaction* tx,
                                   const std::string& index_name,
                                   const std::string& secondary_key,
                                   const std::string& primary_key);
    bool tx_update_secondary_index(LineairDBTransaction* tx,
                                   const std::string& index_name,
                                   const std::string& old_secondary_key,
                                   const std::string& new_secondary_key,
                                   const std::string& primary_key);
    std::vector<std::string> tx_scan_secondary_index(LineairDBTransaction* tx,
                                                     const std::string& index_name,
                                                     const std::string& start_key,
                                                     const std::string& end_key,
                                                     const std::string& exclusive_end_key,
                                                     bool reverse,
                                                     uint32_t limit);
    std::vector<SecondaryIndexEntry> tx_scan_secondary_index_with_keys(LineairDBTransaction* tx,
                                                                       const std::string& index_name,
                                                                       const std::string& start_key,
                                                                       const std::string& end_key,
                                                                       const std::string& exclusive_end_key,
                                                                       bool reverse,
                                                                       uint32_t limit);

    // table/index management (non-transactional)
    bool db_create_table(const std::string& table_name);
    bool db_set_table(int64_t tx_id, const std::string& table_name);
    bool db_create_secondary_index(const std::string& table_name,
                                   const std::string& index_name,
                                   uint32_t index_type);

    // database operations
    bool db_end_transaction(int64_t tx_id, bool isFence);
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

#endif // LINEAIRDB_PROXY_H
