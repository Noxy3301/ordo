#include "wire.hh"

#include <cstring>
#include <endian.h>
#include <arpa/inet.h>

namespace bench_client {

void pack_header(uint64_t sender_id, MessageType type, uint32_t payload_size, void* out) {
    MessageHeader hdr;
    hdr.sender_id = htobe64(sender_id);
    hdr.message_type = htonl(static_cast<uint32_t>(type));
    hdr.payload_size = htonl(payload_size);
    std::memcpy(out, &hdr, sizeof(hdr));
}

void unpack_header(const void* in, uint64_t* sender_id, MessageType* type, uint32_t* payload_size) {
    MessageHeader hdr;
    std::memcpy(&hdr, in, sizeof(hdr));
    *sender_id = be64toh(hdr.sender_id);
    *type = static_cast<MessageType>(ntohl(hdr.message_type));
    *payload_size = ntohl(hdr.payload_size);
}

}  // namespace bench_client
