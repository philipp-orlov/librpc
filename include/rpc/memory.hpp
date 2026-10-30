// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

#pragma once

// Where received messages land.
//
// By default every received frame is copied once, from the socket or the shared-memory ring, into a block of
// bufs::BufferPool, and the handler (or the caller of a client) reads the message in place. A GPU cannot read
// that memory. Two ways to make the one copy land in memory it can read, so there is no second copy:
//
// 1. InboundMemory: the caller supplies allocate/release callbacks and decides everything (a pinned pool, CUDA
//    managed memory, a per-device assignment, ...).
// 2. PinnedPool: librpc keeps a grow-only pool of page-aligned blocks and calls caller-supplied register /
//    unregister callbacks once per block (cudaHostRegister / cudaHostUnregister), so a block is registered when
//    it is first created and stays registered while it is reused. No CUDA headers are needed here.
//
// Either goes in ClientOptions::inboundMemory or EndpointOptions::inboundMemory(). The copy out of a local
// connection's ring stays: the peer is another process and may change the ring under us, so the ring itself
// is never handed out (registering it would not make reading it safe).

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <vector>

namespace rpc {

/// Caller-supplied memory for received frames. A frame is the envelope followed by the message, so a message
/// sits a few dozen bytes past the start of its block.
struct InboundMemory {
    /// At least `bytes` bytes, 8-aligned, readable until released. Called on I/O threads, concurrently. Returning
    /// nullptr (or throwing) makes librpc use its own pool for that frame.
    void* (*allocate)(void* ctx, size_t bytes) = nullptr;
    /// Gives a block back; `bytes` is what allocate() was asked for. Called on any thread, when the last user of
    /// the message lets go (a handler returning, a Payload destroyed). It may run after the client or endpoint
    /// is gone, so `ctx` must outlive every message.
    void (*release)(void* ctx, void* data, size_t bytes) = nullptr;
    void* ctx = nullptr;

    explicit operator bool() const noexcept { return allocate != nullptr && release != nullptr; }
};

/// A grow-only pool of page-aligned blocks, registered with a device on creation. Power-of-two size classes from
/// 4 KiB; a released block goes to its class's free list and is reused, never freed or re-registered until the
/// pool is destroyed, so the steady state has no allocation, no registration and no fragmentation.
///
/// CUDA:
///   PinnedPool pool({[](void*, void* p, size_t n) { return cudaHostRegister(p, n, cudaHostRegisterDefault) == cudaSuccess; },
///                    [](void*, void* p, size_t) { cudaHostUnregister(p); }, nullptr});
///   clientOptions.inboundMemory = pool.memory();
///
/// The pool must outlive every message received through it.
class PinnedPool {
public:
    struct Registration {
        /// Registers [data, data + bytes); false = it could not be (that frame falls back to librpc's own pool).
        bool (*registerMemory)(void* ctx, void* data, size_t bytes) = nullptr;
        /// Undoes it. Called for every registered block when the pool is destroyed.
        void (*unregisterMemory)(void* ctx, void* data, size_t bytes) = nullptr;
        void* ctx = nullptr;
    };

    PinnedPool() = default;
    explicit PinnedPool(Registration registration);
    ~PinnedPool();
    PinnedPool(const PinnedPool&) = delete;
    PinnedPool& operator=(const PinnedPool&) = delete;

    /// The allocate/release pair that serves from this pool.
    InboundMemory memory() noexcept;

    /// Blocks created so far, their total bytes, and how many are out right now.
    size_t blocks() const;
    size_t bytes() const;
    size_t inUse() const noexcept { return inUse_.load(std::memory_order_relaxed); }

private:
    static constexpr size_t kMinShift = 12;  // 4 KiB
    static constexpr size_t kClasses = 53;

    void* take(size_t bytes);
    void give(void* data, size_t bytes);

    Registration registration_;
    mutable std::mutex mutex_;
    std::vector<void*> free_[kClasses];
    std::vector<void*> all_;  // every block, with its class size, for the destructor
    std::vector<size_t> allSizes_;
    std::atomic<size_t> inUse_{0};
};

} // namespace rpc
