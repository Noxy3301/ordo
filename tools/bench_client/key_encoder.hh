#pragma once

// Mirrors `proxy/ha_lineairdb.cc::append_key_part_encoding` +
// `encode_int_key` exactly so LDB sees the same shape of keys as via MySQL.
//
// Wire format per key part:
//   [1B null_marker: 0x00 notnull / 0x01 null]
//   [1B type_tag:    0x10 INT / 0x20 STRING / 0x30 DATETIME / 0xF0 OTHER]
//   INT/DATETIME/OTHER: [2B length BE][N bytes payload]
//   STRING:             [payload bytes][0x00 terminator][2B length BE]
//
// INT payload: flip sign bit of value, then big-endian bytes of chosen width.
//   `encode_int32(1)`  → 80 00 00 01  (4 bytes)
//   `encode_int32(-1)` → 7f ff ff ff
//
// Composite keys are simple concatenation of part encodings.

#include <cstdint>
#include <string>

namespace bench_client {

class KeyBuilder {
public:
    KeyBuilder& int32(int32_t v);
    KeyBuilder& int64(int64_t v);
    KeyBuilder& string(const std::string& s);

    const std::string& build() const { return buf_; }
    std::string take() { return std::move(buf_); }

private:
    std::string buf_;
};

// Convenience: build composite key from any number of INT parts.
template <typename... Ts>
std::string make_int_key(Ts... parts) {
    KeyBuilder kb;
    (kb.int32(static_cast<int32_t>(parts)), ...);
    return kb.take();
}

}  // namespace bench_client
