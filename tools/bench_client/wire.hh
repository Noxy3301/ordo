#pragma once

#include "protocol/message.hh"

#include <cstdint>
#include <string>

namespace bench_client {

// Pack a MessageHeader into a 16-byte buffer in network byte order.
// `out` must point to at least sizeof(MessageHeader) = 16 bytes.
void pack_header(uint64_t sender_id, MessageType type, uint32_t payload_size, void* out);

// Unpack a 16-byte buffer (network byte order) into host-byte-order fields.
void unpack_header(const void* in, uint64_t* sender_id, MessageType* type, uint32_t* payload_size);

}  // namespace bench_client
