#include "client.hh"
#include "wire.hh"

#include <arpa/inet.h>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

namespace bench_client {

Client::Client() = default;

Client::~Client() { disconnect(); }

bool Client::connect(const std::string& host, int port) {
    disconnect();

    fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd_ < 0) {
        std::fprintf(stderr, "bench_client: socket() failed: %s\n", std::strerror(errno));
        return false;
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(port));
    if (inet_pton(AF_INET, host.c_str(), &addr.sin_addr) <= 0) {
        std::fprintf(stderr, "bench_client: inet_pton(%s) failed\n", host.c_str());
        ::close(fd_);
        fd_ = -1;
        return false;
    }

    if (::connect(fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        std::fprintf(stderr, "bench_client: connect(%s:%d) failed: %s\n", host.c_str(), port,
                     std::strerror(errno));
        ::close(fd_);
        fd_ = -1;
        return false;
    }

    int flag = 1;
    setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));

    return true;
}

void Client::disconnect() {
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
}

bool Client::call(MessageType type, const google::protobuf::MessageLite& req,
                  google::protobuf::MessageLite& resp) {
    if (fd_ < 0) return false;

    // Serialize request into send_buf_ = [16B header][payload].
    const size_t payload_size = req.ByteSizeLong();
    send_buf_.resize(sizeof(MessageHeader) + payload_size);
    pack_header(1, type, static_cast<uint32_t>(payload_size), send_buf_.data());
    if (payload_size > 0 &&
        !req.SerializeToArray(send_buf_.data() + sizeof(MessageHeader),
                              static_cast<int>(payload_size))) {
        std::fprintf(stderr, "bench_client: SerializeToArray failed\n");
        return false;
    }

    // Send header+payload; handle partial writes.
    size_t sent = 0;
    while (sent < send_buf_.size()) {
        ssize_t n = ::send(fd_, send_buf_.data() + sent, send_buf_.size() - sent, 0);
        if (n <= 0) {
            std::fprintf(stderr, "bench_client: send() failed: %s\n", std::strerror(errno));
            return false;
        }
        sent += static_cast<size_t>(n);
    }

    // Receive response header (MSG_WAITALL).
    char header_buf[sizeof(MessageHeader)];
    ssize_t got = ::recv(fd_, header_buf, sizeof(header_buf), MSG_WAITALL);
    if (got != static_cast<ssize_t>(sizeof(header_buf))) {
        std::fprintf(stderr, "bench_client: recv(header) got %zd\n", got);
        return false;
    }

    uint64_t resp_sender_id;
    MessageType resp_type;
    uint32_t resp_payload_size;
    unpack_header(header_buf, &resp_sender_id, &resp_type, &resp_payload_size);

    // Sanity: server echoes the request MessageType in its response. If not,
    // the stream is desynced and continuing would silently misparse bytes.
    if (resp_type != type) {
        std::fprintf(stderr, "bench_client: response type mismatch req=%u resp=%u\n",
                     static_cast<uint32_t>(type), static_cast<uint32_t>(resp_type));
        return false;
    }

    // Receive payload (MSG_WAITALL).
    recv_buf_.resize(resp_payload_size);
    if (resp_payload_size > 0) {
        got = ::recv(fd_, recv_buf_.data(), resp_payload_size, MSG_WAITALL);
        if (got != static_cast<ssize_t>(resp_payload_size)) {
            std::fprintf(stderr, "bench_client: recv(payload) got %zd want %u\n", got,
                         resp_payload_size);
            return false;
        }
    }

    if (!resp.ParseFromArray(recv_buf_.data(), static_cast<int>(resp_payload_size))) {
        std::fprintf(stderr, "bench_client: ParseFromArray failed (type=%u size=%u)\n",
                     static_cast<uint32_t>(resp_type), resp_payload_size);
        return false;
    }
    return true;
}

bool Client::call_flat_scan(MessageType type,
                            const google::protobuf::MessageLite& req,
                            bool* is_aborted,
                            std::vector<std::pair<std::string, std::string>>* out_kvs) {
    if (fd_ < 0) return false;

    // Serialize request identically to call().
    const size_t payload_size = req.ByteSizeLong();
    send_buf_.resize(sizeof(MessageHeader) + payload_size);
    pack_header(1, type, static_cast<uint32_t>(payload_size), send_buf_.data());
    if (payload_size > 0 &&
        !req.SerializeToArray(send_buf_.data() + sizeof(MessageHeader),
                              static_cast<int>(payload_size))) {
        std::fprintf(stderr, "bench_client: SerializeToArray failed\n");
        return false;
    }
    size_t sent = 0;
    while (sent < send_buf_.size()) {
        ssize_t n = ::send(fd_, send_buf_.data() + sent, send_buf_.size() - sent, 0);
        if (n <= 0) return false;
        sent += static_cast<size_t>(n);
    }
    char header_buf[sizeof(MessageHeader)];
    ssize_t got = ::recv(fd_, header_buf, sizeof(header_buf), MSG_WAITALL);
    if (got != static_cast<ssize_t>(sizeof(header_buf))) return false;
    uint64_t resp_sender_id;
    MessageType resp_type;
    uint32_t resp_payload_size;
    unpack_header(header_buf, &resp_sender_id, &resp_type, &resp_payload_size);
    if (resp_type != type) {
        std::fprintf(stderr, "bench_client: flat_scan response type mismatch %u != %u\n",
                     static_cast<uint32_t>(resp_type), static_cast<uint32_t>(type));
        return false;
    }
    recv_buf_.resize(resp_payload_size);
    if (resp_payload_size > 0) {
        got = ::recv(fd_, recv_buf_.data(), resp_payload_size, MSG_WAITALL);
        if (got != static_cast<ssize_t>(resp_payload_size)) return false;
    }

    // Parse flat format: [1B aborted] ([4B klen][key][4B vlen][value])* [4B sentinel=0]
    if (recv_buf_.empty()) return false;
    *is_aborted = recv_buf_[0] != 0;
    out_kvs->clear();
    size_t p = 1;
    const size_t end = recv_buf_.size();
    while (p + 4 <= end) {
        uint32_t klen;
        std::memcpy(&klen, recv_buf_.data() + p, 4);
        p += 4;
        if (klen == 0) break;  // sentinel
        if (p + klen + 4 > end) return false;
        std::string key(recv_buf_.data() + p, klen);
        p += klen;
        uint32_t vlen;
        std::memcpy(&vlen, recv_buf_.data() + p, 4);
        p += 4;
        if (p + vlen > end) return false;
        std::string val(recv_buf_.data() + p, vlen);
        p += vlen;
        out_kvs->emplace_back(std::move(key), std::move(val));
    }
    return true;
}

}  // namespace bench_client
