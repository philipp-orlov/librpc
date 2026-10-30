// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

#pragma once

// Shared blobs: zero-copy bulk data over a local socket (README, "Shared blobs").
//
// A `[ubyte]` field marked `(shared)` in the schema is written once, by the client, into memory the server reads in
// place. The client asks for a block of the connection's shared area with Client::allocateShared(), fills it and hands
// it to a call; the server's handler gets a Blob. Over a WebSocket, or against a server that does not offer a shared
// area, the same code works: the field travels inline and Blob::shared is false.
//
// The bytes of a shared blob are opaque to librpc and the client may change them while the server reads them. Use
// (shared) for data the server would accept arbitrary and changing (raw pixels, audio samples), never for bytes it
// parses (JPEG, JSON, nested messages).

#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>

#include "bufs/pool.hpp"
#include "rpc/span.hpp"

namespace rpc {

namespace detail {
class ClientArena;
struct ClientImpl;
class Session;
} // namespace detail

/// Registers memory with a device. Returns the address the device reads `data` at (== data where addressing is
/// unified), or nullptr when it cannot be registered. Called once per arena (server) or per PinnedPool block.
struct DeviceRegistration {
    void* (*registerMemory)(void* ctx, void* data, size_t bytes) = nullptr;
    void (*unregisterMemory)(void* ctx, void* data, size_t bytes) = nullptr;
    void* ctx = nullptr;
    explicit operator bool() const noexcept { return registerMemory && unregisterMemory; }
};

/// What a server handler gets for a shared field: valid for the handler call (or as long as the Taken<T> it came
/// from lives).
struct Blob {
    const uint8_t* data = nullptr;
    size_t size = 0;
    /// Where a device reads `data`; nullptr when it is not device-visible (an inline field that did not land in
    /// registered memory). Equal to `data` on unified-address systems.
    const void* device = nullptr;
    /// True when the bytes lie in the client's shared area (they may change under you, see the docs).
    bool shared = false;
};

/// Client side: a block of the connection's shared area, or a pooled fallback that will be sent inline. Move-only;
/// its memory returns to the arena when the server releases it (never earlier), or when it is destroyed unsent.
class SharedBuffer {
public:
    SharedBuffer() noexcept = default;
    ~SharedBuffer();
    SharedBuffer(SharedBuffer&& other) noexcept;
    SharedBuffer& operator=(SharedBuffer&& other) noexcept;
    SharedBuffer(const SharedBuffer&) = delete;
    SharedBuffer& operator=(const SharedBuffer&) = delete;

    uint8_t* data() noexcept { return data_; }
    const uint8_t* data() const noexcept { return data_; }
    size_t size() const noexcept { return size_; }
    /// False: a pooled buffer that will travel inline.
    bool shared() const noexcept { return arena_ != nullptr; }
    /// Where a device reads data() through the client-side registration (ClientOptions::sharedRegistration);
    /// nullptr without one.
    const void* device() const noexcept;
    explicit operator bool() const noexcept { return data_ != nullptr; }

private:
    friend class Client;
    friend struct detail::ClientImpl;
    friend class detail::ClientArena;

    /// Gives up the arena block to the connection: it is on the wire now and returns on the server's release.
    /// Returns its offset.
    uint64_t markSent() noexcept;
    void reset() noexcept;

    std::shared_ptr<detail::ClientArena> arena_;
    uint64_t offset_ = 0;
    uint8_t* data_ = nullptr;
    size_t size_ = 0;
    bufs::Buffer pooled_;
};

/// A generated client stub's parameter for a shared field: a SharedBuffer (zero-copy) or a plain span (inline).
class Shared {
public:
    Shared(SharedBuffer&& buffer) noexcept : buffer_(std::move(buffer)) {}                         // NOLINT: implicit on purpose
    Shared(Span<const uint8_t> bytes) noexcept : bytes_(bytes) {}                                  // NOLINT
    Shared(Span<uint8_t> bytes) noexcept : bytes_(bytes.data(), bytes.size()) {}                    // NOLINT
    Shared() noexcept = default;

    /// True: the bytes are in the shared area and go by reference.
    bool zeroCopy() const noexcept { return buffer_.shared(); }
    /// The bytes as an inline field: the span given, or a pooled buffer's.
    Span<const uint8_t> inlineBytes() const noexcept {
        return buffer_ ? Span<const uint8_t>(buffer_.data(), buffer_.size()) : bytes_;
    }
    SharedBuffer& buffer() noexcept { return buffer_; }

private:
    SharedBuffer buffer_;
    Span<const uint8_t> bytes_;
};

/// For hand-written stream writes: attach a SharedBuffer to field slot `field` of the item (the rpcc constant
/// <Table>Shared::<name>).
struct Share {
    uint32_t field = 0;
    SharedBuffer buffer;
};

/// Blobs a message may carry by reference.
inline constexpr size_t kMaxBlobsPerMessage = 4;

namespace detail {
/// For the generated client stubs: moves `value`'s buffer into the next free share when it is a block of the shared
/// area (true); false when it must be sent inline.
inline bool takeShare(Share* shares, size_t& count, uint32_t field, Shared& value) noexcept {
    if (!value.zeroCopy())
        return false;
    shares[count].field = field;
    shares[count].buffer = std::move(value.buffer());
    ++count;
    return true;
}
} // namespace detail

} // namespace rpc
