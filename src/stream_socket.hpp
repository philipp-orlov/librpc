// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// The local transport on Windows: an AF_UNIX stream socket carrying the same frames as the WebSocket and the shared-memory
// rings, each behind a 4-byte length. Windows has no sealed anonymous shared memory, no descriptor passing and no cross-process
// futex, so the rings and the shared area (rpc/shared.hpp) are not available there; everything else about a local connection
// (unix:// URLs, listenLocal, Handshake with the peer's identity, authorize) is the same.
#pragma once

#if !defined(_WIN32)
#error "stream_socket.hpp is the Windows local transport"
#endif

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "sys/net.hpp"

namespace rpc::detail {

/// Who is on the other end of a local connection, as the kernel and the access token say.
struct PeerIdentity {
    uint32_t pid = 0;
    /// The peer runs as the same account as this process, or is an elevated administrator. The Windows reading of "the
    /// owner": the server sets Handshake::uid to 0 for such a peer and to -1 for any other.
    bool owner = false;
};

class StreamSocket {
public:
    StreamSocket() = default;
    explicit StreamSocket(sys::socket_t handle) : handle_(handle) {}
    ~StreamSocket() { reset(); }
    StreamSocket(StreamSocket&& other) noexcept : handle_(other.handle_) { other.handle_ = sys::kInvalidSocket; }
    StreamSocket& operator=(StreamSocket&& other) noexcept {
        if (this != &other) {
            reset();
            handle_ = other.handle_;
            other.handle_ = sys::kInvalidSocket;
        }
        return *this;
    }
    StreamSocket(const StreamSocket&) = delete;
    StreamSocket& operator=(const StreamSocket&) = delete;

    /// Connects to the socket file at `path`. Throws std::runtime_error.
    static StreamSocket connect(const std::string& path, int timeoutMillis);
    /// Listens at `path`. A socket file left by a process that died is replaced; one another process listens on is an error.
    /// The listener is non-blocking. Throws std::runtime_error.
    static StreamSocket listen(const std::string& path);

    /// A new connection (blocking), or an empty socket when none waits.
    StreamSocket accept() const;
    /// Whether it can be read from without blocking (also true at the end of the stream), within `millis`.
    bool waitReadable(int millis) const;

    /// All of it, or throws std::runtime_error (the peer has gone, or a send stalled past the send timeout).
    void sendAll(const void* data, size_t size) const;
    /// Gives up on a send that stalls this long.
    void setSendTimeout(int millis) const;
    /// Exactly `size` bytes within `timeoutMillis`; false on timeout or when the peer closed first.
    bool receiveExactly(void* data, size_t size, int timeoutMillis) const;
    /// One recv: bytes read, 0 at the end of the stream, throws on an error.
    long receiveSome(void* data, size_t size) const;

    PeerIdentity peer() const;
    /// Ends the connection both ways; a thread waiting on it wakes.
    void shutdown() const;

    sys::socket_t handle() const { return handle_; }
    explicit operator bool() const { return handle_ != sys::kInvalidSocket; }

private:
    void reset() noexcept;

    sys::socket_t handle_ = sys::kInvalidSocket;
};

/// Removes a socket file; true when it is gone.
bool removeSocketFile(const std::string& path) noexcept;

/// Cuts a stream into frames: a 4-byte little-endian length, then that many bytes.
class FrameReader {
public:
    enum class Result { Frame, Timeout, Closed };

    /// The next frame into `frame`, waiting up to `timeoutMillis` for it. Throws std::runtime_error for a frame longer than
    /// `maxFrame` or a socket error. A frame split across calls is kept.
    Result read(const StreamSocket& socket, int timeoutMillis, std::vector<uint8_t>& frame, size_t maxFrame);

private:
    std::vector<uint8_t> buffer_;
    size_t position_ = 0;  // start of the unread part of buffer_
};

/// Sends one frame: `envelope` then `body` behind their joint length. Thread safety is the caller's.
void sendFrame(const StreamSocket& socket, std::vector<uint8_t>& scratch, const uint8_t* envelope, size_t envelopeSize,
               const uint8_t* body, size_t bodySize);

} // namespace rpc::detail
