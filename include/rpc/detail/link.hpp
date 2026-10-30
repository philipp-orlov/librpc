// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// librpc internals: what the protocol core needs of a transport. A link carries whole frames (an Envelope and
// an optional payload message, see frame.hpp) in order, in both directions. Two exist: a WebSocket connection
// (libhttp) and a pair of shared-memory rings set up over a local socket (libbufs).
#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string_view>

#include "bufs/bufs.hpp"
#include "bufs/fd.hpp"
#include "rpc/detail/block.hpp"

namespace rpc::detail {

/// The transport failed or was closed; what() says how.
class LinkError : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

/// A received frame, in 8-aligned memory the receiver owns (pooled, or the caller's InboundMemory).
struct Inbound {
    Block buffer;
    size_t size = 0;
};

/// The client's end.
class ClientLink {
public:
    virtual ~ClientLink() = default;

    /// Sends one frame: `envelope` then `body` (which may be empty). Thread-safe; throws LinkError.
    virtual void send(const uint8_t* envelope, size_t envelopeSize, const uint8_t* body, size_t bodySize) = 0;
    /// The next frame, or false when `deadline` passed first. Throws LinkError when the link is gone.
    /// One thread at a time.
    virtual bool receive(Inbound& frame, std::chrono::steady_clock::time_point deadline) = 0;
    /// Ends the link and wakes a thread blocked in receive(). Any thread.
    virtual void close() noexcept = 0;
    /// The largest frame the link can carry, or 0 for no limit of its own.
    virtual size_t maxFrame() const noexcept { return 0; }
    /// Local links: the largest shared area, in MiB, the server accepts; 0 = none offered (and always 0 elsewhere).
    virtual uint64_t offeredArenaMiB() const noexcept { return 0; }
    /// Local links: hands the server a shared area (a sealed memfd of `bytes`) before the Hello that announces it.
    virtual void sendArena(uint64_t bytes, int fd) {
        (void)bytes;
        (void)fd;
    }
};

/// The server's end of one connection. The protocol core (Session) calls these; the transport calls the
/// Session when a frame arrives or the connection ends.
class ServerLink {
public:
    virtual ~ServerLink() = default;

    /// Queues one frame for the peer: `envelope` then `body`. Any thread; frames keep their order. False when
    /// the link is closed.
    virtual bool send(const uint8_t* envelope, size_t envelopeSize, const uint8_t* body, size_t bodySize) = 0;
    /// Flushes what was queued, then closes. Any thread.
    virtual void close(uint16_t code, std::string_view reason) = 0;
    virtual std::string_view remoteAddress() const = 0;
    /// The largest frame this connection can carry, or 0 for the endpoint's own limit.
    virtual size_t maxFrame() const noexcept { return 0; }
    /// True when frames arrive on a thread of the link's own that may run a short handler (IPC); a WebSocket's
    /// frames arrive on a shared I/O thread, which must never.
    virtual bool dedicatedReader() const noexcept { return false; }
    /// Local links: takes, without blocking, the shared area the client sent before its Hello (the memfd), after
    /// checking that it announced `bytes`. False when there is none or it is malformed (the fd is closed). Links that
    /// cannot carry one -- a WebSocket -- always say false.
    virtual bool takeArena(uint64_t bytes, bufs::Fd& fd) {
        (void)bytes;
        (void)fd;
        return false;
    }
};

} // namespace rpc::detail
