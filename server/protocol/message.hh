#pragma once

#include <cstdint>

// Message header for RPC communication
struct MessageHeader {
    uint64_t sender_id;      // sender ID (not used in LineairDB but keeping for consistency)
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
    TX_SCAN = 5,    // [deprecated] Legacy scan API. Superseded by TX_SCAN_RANGE.
    DB_FENCE = 6,
    DB_END_TRANSACTION = 7,
    TX_DELETE = 8,
    TX_SCAN_RANGE = 9,
    TX_READ_SECONDARY_INDEX = 10,
    TX_WRITE_SECONDARY_INDEX = 11,
    TX_DELETE_SECONDARY_INDEX = 12,
    TX_UPDATE_SECONDARY_INDEX = 13,
    TX_SCAN_SECONDARY_INDEX = 14,
    DB_CREATE_TABLE = 15,
    DB_SET_TABLE = 16,
    DB_CREATE_SECONDARY_INDEX = 17
};
