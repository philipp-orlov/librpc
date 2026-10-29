// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// The local-socket handshake shared by the IPC server and client.
//
//   client: connect(2) to the socket file (SOCK_SEQPACKET)
//   server: creates an anonymous shared-memory Channel (two rings), sends IpcHello + the channel's memfd
//           (SCM_RIGHTS) on the socket
//   client: adopts the channel -- its send ring is the server's receive ring and the other way round
//   both:   the same frames as over a WebSocket, written to the rings; the socket only tells each side when
//           the other has gone (and gives the server the peer's pid/uid/gid)
#pragma once

#include <cstdint>

namespace rpc::detail {

struct IpcHello {
    uint32_t magic = kMagic;
    uint32_t version = 1;
    uint64_t ringBytes = 0;  // capacity of each ring
    uint32_t status = 0;     // 0: accepted (a channel fd is attached); else the HTTP-style status of a refusal
    uint32_t reserved = 0;
    static constexpr uint32_t kMagic = 0x43505231;  // "1RPC"
};

/// A power of two ring that carries a message of `maxMessage` bytes (plus the envelope), with room for a few
/// more in flight: the largest message is half the ring.
inline uint64_t ringCapacityFor(uint64_t maxMessage) {
    uint64_t capacity = 1 << 16;
    while (capacity / 2 - 8 < maxMessage + 4096 + 8)
        capacity <<= 1;
    return capacity;
}

} // namespace rpc::detail
