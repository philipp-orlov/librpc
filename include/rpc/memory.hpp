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

#include "rpc/shared.hpp"

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
    /// Optional: where a device reads the byte at `host`, which lies in a block allocate() returned (PinnedPool
    /// fills it in); nullptr when it does not know. Lets a handler ask for the device address of an inline field.
    const void* (*device)(void* ctx, const void* host) = nullptr;

    explicit operator bool() const noexcept { return allocate != nullptr && release != nullptr; }
};

/// A grow-only pool of page-aligned blocks, registered with a device on creation. Power-of-two size classes from
/// 4 KiB; a released block goes to its class's free list and is reused, never freed or re-registered until the
/// pool is destroyed, so the steady state has no allocation, no registration and no fragmentation.
///
/// CUDA:
///   PinnedPool pool({[](void*, void* p, size_t n) { return cudaHostRegister(p, n, cudaHostRegisterDefault) == cudaSuccess ? p : nullptr; },
///                    [](void*, void* p, size_t) { cudaHostUnregister(p); }, nullptr});
///   clientOptions.inboundMemory = pool.memory();
///
/// The registration returns the address the device reads the block at (the block's own address where addressing is
/// unified; on Tegra it may differ). PinnedPool remembers it per block: device() translates.
///
/// The pool must outlive every message received through it.
///
/// Unbounded by default, and then it retains the high-water mark of every size class for its whole life. Limits bound
/// that, and neither touches a block that is out:
///   maxBytes      registered bytes (idle and out together). A request that would pass it is refused (nullptr), so
///                 that frame falls back to librpc's own pool, unregistered.
///   maxIdleBytes  bytes kept registered for reuse; a block released beyond it is unregistered and freed at once,
///                 largest classes first. trim() does the same on demand.
/// The shared arenas (EndpointOptions::sharedArena*) are budgeted separately: this pool is not part of them.
class PinnedPool {
public:
    struct Limits {
        size_t maxBytes = 0;      // 0 = none
        size_t maxIdleBytes = 0;  // 0 = none
    };
    struct Stats {
        size_t blocks = 0;          // registered blocks
        size_t bytes = 0;           // their total size
        size_t inUseBlocks = 0;     // out right now
        size_t idleBytes = 0;       // registered and waiting for reuse
        size_t peakBytes = 0;       // the most registered at once
        uint64_t refusals = 0;      // requests refused by maxBytes (they fell back to the library's pool)
        uint64_t evictedBlocks = 0; // idle blocks unregistered by maxIdleBytes or trim()
    };

    PinnedPool() = default;
    /// A non-null result of registerMemory is the block's device address; nullptr = it could not be registered (that
    /// frame falls back to librpc's own pool). unregisterMemory is called for every registered block when the pool
    /// is destroyed.
    explicit PinnedPool(DeviceRegistration registration);
    PinnedPool(DeviceRegistration registration, Limits limits);
    ~PinnedPool();
    PinnedPool(const PinnedPool&) = delete;
    PinnedPool& operator=(const PinnedPool&) = delete;

    /// The allocate/release pair that serves from this pool.
    InboundMemory memory() noexcept;

    /// The device address of the byte at `host`, which lies inside one of this pool's blocks; nullptr when it does
    /// not, or the pool has no registration.
    const void* device(const void* host) const;

    /// Blocks created so far, their total bytes, and how many are out right now.
    size_t blocks() const;
    size_t bytes() const;
    size_t inUse() const noexcept { return inUse_.load(std::memory_order_relaxed); }
    Stats stats() const;
    /// Unregisters and frees idle blocks, largest first, until at most `keepIdleBytes` of them remain. Returns the
    /// bytes freed. Blocks that are out are never touched.
    size_t trim(size_t keepIdleBytes = 0);

private:
    static constexpr size_t kMinShift = 12;  // 4 KiB
    static constexpr size_t kClasses = 53;

    struct BlockInfo {
        uintptr_t host;
        size_t bytes;
        void* device;
    };

    void* take(size_t bytes);
    void give(void* data, size_t bytes);
    // Detaches idle blocks past `keep` bytes (under the lock held by the caller); retire() runs outside it.
    void evictLocked(size_t keep, std::vector<BlockInfo>& out);
    void retire(const std::vector<BlockInfo>& blocks);

    DeviceRegistration registration_;
    Limits limits_;
    mutable std::mutex mutex_;
    std::vector<void*> free_[kClasses];
    std::vector<BlockInfo> all_;  // every block, sorted by host address (grows only when the pool does)
    std::atomic<size_t> inUse_{0};
    size_t registeredBytes_ = 0;  // all_, in bytes
    size_t reservedBytes_ = 0;    // being created right now, outside the lock
    size_t idleBytes_ = 0;
    size_t peakBytes_ = 0;
    uint64_t refusals_ = 0;
    uint64_t evicted_ = 0;
};

} // namespace rpc
