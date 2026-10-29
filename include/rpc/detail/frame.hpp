// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// librpc internals shared by the server and the client: building and parsing frames.
// A frame is an Envelope message, optionally followed by one more message (the call's payload), all 8-aligned.
#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string_view>
#include <utility>

#include "bufs/bufs.hpp"
#include "rpc/wire/rpc.bufs.hpp"

namespace rpc::detail {

inline constexpr const char* kSubprotocol = "librpc.v1";
inline constexpr uint16_t kProtocol = 1;

/// Bytes of a status message that travel: a longer one is cut.
inline constexpr size_t kMaxStatusMessage = 1024;

/// A frame taken apart. `body` is empty for a frame that is only an envelope.
struct Frame {
    wire::Envelope::Reader envelope;
    const uint8_t* bodyData = nullptr;
    size_t bodySize = 0;
};

/// Checks and splits a received frame (8-aligned memory). False when it is not an Envelope, does not verify, or
/// is followed by something that is not exactly one message.
inline bool parseFrame(const uint8_t* data, size_t size, Frame& out) {
    if (size < bufs::kHeaderBytes || (reinterpret_cast<uintptr_t>(data) & 7) != 0)
        return false;
    if (bufs::typeOf(data, size) != wire::Envelope::typeId)
        return false;
    uint32_t envelopeSize = 0;
    std::memcpy(&envelopeSize, data, sizeof envelopeSize);
    if (envelopeSize < bufs::kHeaderBytes || envelopeSize % 8 != 0 || envelopeSize > size)
        return false;
    if (!bufs::verify<wire::Envelope>(data, envelopeSize))
        return false;
    out.envelope = bufs::root<wire::Envelope>(data);
    out.bodyData = nullptr;
    out.bodySize = 0;
    if (envelopeSize < size) {
        uint32_t bodySize = 0;
        std::memcpy(&bodySize, data + envelopeSize, sizeof bodySize);
        if (size - envelopeSize < bufs::kHeaderBytes || bodySize != size - envelopeSize || bodySize % 8 != 0)
            return false;
        out.bodyData = data + envelopeSize;
        out.bodySize = bodySize;
    }
    return true;
}

/// Builds an Envelope with `fill(wire::Envelope::Builder&)`. The bytes belong to a per-thread builder and are
/// valid until the next envelope() on this thread.
template <class Fill>
std::pair<const uint8_t*, size_t> envelope(Fill&& fill) {
    thread_local bufs::MessageBuilder builder;
    builder.clear();
    wire::Envelope::Builder root = builder.initRoot<wire::Envelope>();
    fill(root);
    return builder.finish();
}

inline std::string_view clipMessage(std::string_view text) {
    return text.size() <= kMaxStatusMessage ? text : text.substr(0, kMaxStatusMessage);
}

/// `size` bytes copied into 8-aligned, pooled memory.
inline bufs::Buffer alignedCopy(const uint8_t* data, size_t size) {
    bufs::Buffer buffer = bufs::BufferPool::global().acquire(size);
    if (size)
        std::memcpy(buffer.data(), data, size);
    return buffer;
}

} // namespace rpc::detail
