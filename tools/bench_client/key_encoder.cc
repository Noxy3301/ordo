#include "key_encoder.hh"

#include <cstring>

namespace bench_client {

namespace {

constexpr unsigned char kKeyMarkerNotNull = 0x00;
constexpr unsigned char kKeyTypeInt = 0x10;
constexpr unsigned char kKeyTypeString = 0x20;

// encode_int_key equivalent: LE→sign-flip→BE of `len` bytes.
std::string encode_int_be_signflip(uint64_t raw_le, size_t width) {
    uint64_t v = raw_le;
    switch (width) {
        case 1: v ^= 0x80ULL; break;
        case 2: v ^= 0x8000ULL; break;
        case 4: v ^= 0x80000000ULL; break;
        case 8: v ^= 0x8000000000000000ULL; break;
        default: return {};
    }
    std::string out(width, '\0');
    for (size_t i = 0; i < width; ++i) {
        out[i] = static_cast<char>((v >> ((width - 1 - i) * 8)) & 0xFF);
    }
    return out;
}

void append_int_part(std::string& out, uint64_t raw_le, size_t width) {
    std::string payload = encode_int_be_signflip(raw_le, width);
    out.reserve(out.size() + 4 + payload.size());
    out.push_back(static_cast<char>(kKeyMarkerNotNull));
    out.push_back(static_cast<char>(kKeyTypeInt));
    const uint16_t len = static_cast<uint16_t>(payload.size());
    out.push_back(static_cast<char>((len >> 8) & 0xFF));
    out.push_back(static_cast<char>(len & 0xFF));
    out.append(payload);
}

}  // namespace

KeyBuilder& KeyBuilder::int32(int32_t v) {
    // Treat as unsigned for bit manipulation; XOR handles sign extension.
    uint64_t raw = static_cast<uint64_t>(static_cast<uint32_t>(v));
    append_int_part(buf_, raw, 4);
    return *this;
}

KeyBuilder& KeyBuilder::int64(int64_t v) {
    uint64_t raw = static_cast<uint64_t>(v);
    append_int_part(buf_, raw, 8);
    return *this;
}

KeyBuilder& KeyBuilder::string(const std::string& s) {
    const size_t copy_len =
        (s.size() > 65535) ? 65535 : s.size();
    buf_.reserve(buf_.size() + 5 + copy_len);
    buf_.push_back(static_cast<char>(kKeyMarkerNotNull));
    buf_.push_back(static_cast<char>(kKeyTypeString));
    if (copy_len > 0) buf_.append(s.data(), copy_len);
    buf_.push_back('\0');
    const uint16_t len = static_cast<uint16_t>(copy_len);
    buf_.push_back(static_cast<char>((len >> 8) & 0xFF));
    buf_.push_back(static_cast<char>(len & 0xFF));
    return *this;
}

}  // namespace bench_client
