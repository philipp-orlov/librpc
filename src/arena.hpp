// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// The shared area of a local connection (see rpc/shared.hpp): the client's allocator over a sealed memfd it created,
// and the server's read-only view of it.
#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "bufs/fd.hpp"
#include "bufs/shm.hpp"
#include "rpc/shared.hpp"

namespace rpc::detail {

inline constexpr size_t kArenaPage = 4096;

/// The client's end: one sealed memfd, mapped read-write, carved into power-of-two blocks (4 KiB and up).
///
/// Blocks come from a bump pointer on first need and are never returned to it, only to their class's free list, so
/// the steady state carves nothing and does not fragment. A side table indexed by offset / 4 KiB records each
/// block's class and state (so a block can be released by its offset alone) and chains the free lists; it is
/// allocated once. A block is Held while the application owns it (SharedBuffer), Sent while it is on the wire and the
/// server may read it, and returns to its free list from either.
class ClientArena {
public:
    /// Creates `bytes` (rounded up to a page) of shared memory; `registration`, if set, registers it with a device.
    /// Throws std::system_error.
    static std::shared_ptr<ClientArena> create(uint64_t bytes, const DeviceRegistration& registration);
    ~ClientArena();
    ClientArena(const ClientArena&) = delete;
    ClientArena& operator=(const ClientArena&) = delete;

    int fd() const noexcept { return memory_.fd(); }
    uint8_t* base() const noexcept { return memory_.data(); }
    uint64_t bytes() const noexcept { return memory_.size(); }

    /// A block of at least `bytes`; false when none is free (never blocks).
    bool allocate(size_t bytes, uint64_t& offset);
    /// Held -> Sent. False if `offset` is not a Held block.
    bool markSent(uint64_t offset) noexcept;
    /// A Held block the application did not send, back to the free list.
    void releaseHeld(uint64_t offset) noexcept;
    /// A Sent block the server is done with, back to the free list. False if `offset` is not a Sent block (a
    /// misbehaving server; ignored).
    bool releaseSent(uint64_t offset) noexcept;

    /// Bytes of blocks that are Held or Sent.
    uint64_t inUse() const noexcept { return inUse_.load(std::memory_order_relaxed); }
    /// Where a device reads `offset`; nullptr without a registration.
    const void* device(uint64_t offset) const noexcept {
        return device_ ? static_cast<const uint8_t*>(device_) + offset : nullptr;
    }

private:
    ClientArena() = default;

    enum : uint8_t { kFree = 0, kHeld = 1, kSent = 2 };
    struct Entry {
        uint32_t next = 0;  // free list: index + 1 of the next free block of the class, 0 = end
        uint8_t cls = 0;
        uint8_t state = kFree;
    };
    static constexpr size_t kClasses = 53;

    bool release(uint64_t offset, uint8_t expected) noexcept;

    bufs::shm::Memory memory_;
    DeviceRegistration registration_;
    void* device_ = nullptr;
    std::mutex mutex_;
    std::vector<Entry> pages_;       // fixed size: bytes / 4 KiB
    uint32_t head_[kClasses] = {};   // free list heads (index + 1)
    uint64_t bump_ = 0;
    std::atomic<uint64_t> inUse_{0};
};

/// Bytes of shared areas an endpoint has accepted, shared with the areas themselves so that one outliving the
/// endpoint (a Taken<T> held after shutdown) can still give its bytes back.
struct ArenaAccount {
    std::atomic<uint64_t> bytes{0};
    std::atomic<uint64_t> arenas{0};
    uint64_t cap = 0;  // 0 = no cap
    DeviceRegistration registration;
};

/// The server's end: a client's sealed memfd mapped read-only. Reference counted by hand (the session holds one;
/// every blob lease holds one) so that leases cost no allocation; unregisters, unmaps and gives its bytes back when
/// the last reference goes.
class SessionArena {
public:
    /// Validates and maps. nullptr, with `why` set, when the arena is refused. Reserves the bytes in `account`
    /// before registering and gives them back on failure.
    static SessionArena* adopt(bufs::Fd fd, uint64_t bytes, const std::shared_ptr<ArenaAccount>& account, const char*& why);

    void addRef() noexcept { refs_.fetch_add(1, std::memory_order_relaxed); }
    void unref() noexcept {
        if (refs_.fetch_sub(1, std::memory_order_acq_rel) == 1)
            delete this;
    }

    const uint8_t* base() const noexcept { return base_; }
    uint64_t bytes() const noexcept { return bytes_; }
    const void* device(uint64_t offset) const noexcept {
        return device_ ? static_cast<const uint8_t*>(device_) + offset : nullptr;
    }

private:
    SessionArena() = default;
    ~SessionArena();

    std::atomic<uint32_t> refs_{1};
    const uint8_t* base_ = nullptr;
    uint64_t bytes_ = 0;
    void* device_ = nullptr;
    bool registered_ = false;
    std::shared_ptr<ArenaAccount> account_;
};

/// A counted reference to a SessionArena.
class ArenaRef {
public:
    ArenaRef() = default;
    explicit ArenaRef(SessionArena* arena) noexcept : arena_(arena) {
        if (arena_)
            arena_->addRef();
    }
    /// Takes over a reference already counted (SessionArena::adopt's).
    static ArenaRef adopt(SessionArena* arena) noexcept {
        ArenaRef ref;
        ref.arena_ = arena;
        return ref;
    }
    ArenaRef(const ArenaRef& other) noexcept : ArenaRef(other.arena_) {}
    ArenaRef(ArenaRef&& other) noexcept : arena_(other.arena_) { other.arena_ = nullptr; }
    ArenaRef& operator=(ArenaRef other) noexcept {
        std::swap(arena_, other.arena_);
        return *this;
    }
    ~ArenaRef() {
        if (arena_)
            arena_->unref();
    }

    SessionArena* get() const noexcept { return arena_; }
    SessionArena* operator->() const noexcept { return arena_; }
    explicit operator bool() const noexcept { return arena_ != nullptr; }

private:
    SessionArena* arena_ = nullptr;
};

} // namespace rpc::detail
