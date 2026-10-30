// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

#include "arena.hpp"

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>

#include <algorithm>

namespace rpc::detail {

namespace {
size_t classOf(size_t bytes) {
    size_t cls = 0;
    while ((kArenaPage << cls) < bytes)
        ++cls;
    return cls;
}
} // namespace

// ---- ClientArena ------------------------------------------------------------------------------------------------

std::shared_ptr<ClientArena> ClientArena::create(uint64_t bytes, const DeviceRegistration& registration) {
    bytes = (bytes + kArenaPage - 1) / kArenaPage * kArenaPage;
    if (bytes == 0)
        bytes = kArenaPage;
    std::shared_ptr<ClientArena> arena(new ClientArena());
    arena->memory_ = bufs::shm::Memory::create("librpc-arena", static_cast<size_t>(bytes));
    arena->pages_.resize(static_cast<size_t>(bytes / kArenaPage));
    if (registration) {
        arena->registration_ = registration;
        arena->device_ = registration.registerMemory(registration.ctx, arena->memory_.data(), arena->memory_.size());
    }
    return arena;
}

ClientArena::~ClientArena() {
    if (device_ && registration_.unregisterMemory)
        registration_.unregisterMemory(registration_.ctx, memory_.data(), memory_.size());
}

bool ClientArena::allocate(size_t bytes, uint64_t& offset) {
    if (bytes == 0)
        bytes = 1;
    if (bytes > memory_.size())
        return false;
    const size_t cls = classOf(bytes);
    if (cls >= kClasses)
        return false;
    const uint64_t size = uint64_t{kArenaPage} << cls;
    std::lock_guard<std::mutex> lock(mutex_);
    uint64_t at;
    if (head_[cls]) {
        const uint32_t index = head_[cls] - 1;
        head_[cls] = pages_[index].next;
        at = uint64_t{index} * kArenaPage;
    } else if (bump_ + size <= memory_.size()) {
        at = bump_;
        bump_ += size;
    } else if (inUse_.load(std::memory_order_relaxed) == 0 && bump_ != 0) {
        // Nothing is Held or Sent (both counted under this mutex), so every carved block is free and none of the
        // cached classes is worth keeping: start over, and a burst of another size finds the whole area. A block
        // released twice is refused as before, since its entry reads kFree.
        std::fill(std::begin(head_), std::end(head_), 0u);
        bump_ = size;
        at = 0;
    } else {
        return false;
    }
    Entry& entry = pages_[static_cast<size_t>(at / kArenaPage)];
    entry.cls = static_cast<uint8_t>(cls);
    entry.state = kHeld;
    entry.next = 0;
    inUse_.fetch_add(size, std::memory_order_relaxed);
    offset = at;
    return true;
}

bool ClientArena::markSent(uint64_t offset) noexcept {
    std::lock_guard<std::mutex> lock(mutex_);
    if (offset % kArenaPage != 0 || offset >= memory_.size())
        return false;
    Entry& entry = pages_[static_cast<size_t>(offset / kArenaPage)];
    if (entry.state != kHeld)
        return false;
    entry.state = kSent;
    return true;
}

bool ClientArena::release(uint64_t offset, uint8_t expected) noexcept {
    std::lock_guard<std::mutex> lock(mutex_);
    if (offset % kArenaPage != 0 || offset >= memory_.size())
        return false;
    const size_t index = static_cast<size_t>(offset / kArenaPage);
    Entry& entry = pages_[index];
    if (entry.state != expected)
        return false;
    entry.state = kFree;
    entry.next = head_[entry.cls];
    head_[entry.cls] = static_cast<uint32_t>(index + 1);
    inUse_.fetch_sub(uint64_t{kArenaPage} << entry.cls, std::memory_order_relaxed);
    return true;
}

void ClientArena::releaseHeld(uint64_t offset) noexcept { release(offset, kHeld); }
bool ClientArena::releaseSent(uint64_t offset) noexcept { return release(offset, kSent); }

// ---- SessionArena -----------------------------------------------------------------------------------------------

SessionArena* SessionArena::adopt(bufs::Fd fd, uint64_t bytes, const std::shared_ptr<ArenaAccount>& account, const char*& why) {
    why = nullptr;
    if (!fd || bytes == 0 || bytes % kArenaPage != 0) {
        why = "no usable shared area";
        return nullptr;
    }
    struct stat st {};
    if (::fstat(fd.get(), &st) != 0 || !S_ISREG(st.st_mode) || static_cast<uint64_t>(st.st_size) != bytes) {
        why = "the shared area is not the size announced";
        return nullptr;
    }
    const int seals = ::fcntl(fd.get(), F_GET_SEALS);
    constexpr int kWanted = F_SEAL_SHRINK | F_SEAL_GROW | F_SEAL_SEAL;
    if (seals < 0 || (seals & kWanted) != kWanted) {
        why = "the shared area is not sealed";
        return nullptr;
    }
    // Reserve against the cap before pinning anything.
    uint64_t before = account->bytes.load(std::memory_order_relaxed);
    for (;;) {
        if (account->cap && before + bytes > account->cap) {
            why = "the endpoint's shared-area budget is spent";
            return nullptr;
        }
        if (account->bytes.compare_exchange_weak(before, before + bytes, std::memory_order_acq_rel))
            break;
    }
    void* mapped = ::mmap(nullptr, static_cast<size_t>(bytes), PROT_READ, MAP_SHARED, fd.get(), 0);
    if (mapped == MAP_FAILED) {
        account->bytes.fetch_sub(bytes, std::memory_order_acq_rel);
        why = "cannot map the shared area";
        return nullptr;
    }
    std::unique_ptr<SessionArena, void (*)(SessionArena*)> arena(new SessionArena(), [](SessionArena* a) { a->unref(); });
    arena->base_ = static_cast<const uint8_t*>(mapped);
    arena->bytes_ = bytes;
    arena->account_ = account;
    account->arenas.fetch_add(1, std::memory_order_relaxed);
    // From here the destructor undoes the mapping and the reservation.
    if (account->registration) {
        arena->device_ = account->registration.registerMemory(account->registration.ctx, const_cast<uint8_t*>(arena->base_), static_cast<size_t>(bytes));
        if (!arena->device_) {
            why = "the device refused the shared area";
            return nullptr;
        }
        arena->registered_ = true;
    }
    return arena.release();
}

SessionArena::~SessionArena() {
    if (registered_)
        account_->registration.unregisterMemory(account_->registration.ctx, const_cast<uint8_t*>(base_), static_cast<size_t>(bytes_));
    if (base_)
        ::munmap(const_cast<uint8_t*>(base_), static_cast<size_t>(bytes_));
    if (account_) {
        account_->bytes.fetch_sub(bytes_, std::memory_order_acq_rel);
        account_->arenas.fetch_sub(1, std::memory_order_relaxed);
    }
}

} // namespace rpc::detail
