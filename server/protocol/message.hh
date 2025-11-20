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
    TX_SCAN = 5,
    DB_FENCE = 6,
    DB_END_TRANSACTION = 7
};
