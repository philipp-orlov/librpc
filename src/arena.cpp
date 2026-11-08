// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

#include "arena.hpp"

#if defined(_WIN32)

#include <system_error>

namespace rpc::detail {

std::shared_ptr<ClientArena> ClientArena::create(uint64_t, const DeviceRegistration&) {
    throw std::system_error(std::make_error_code(std::errc::function_not_supported), "no shared areas on Windows");
}

ClientArena::~ClientArena() = default;

SessionArena* SessionArena::adopt(bufs::Fd, uint64_t, const std::shared_ptr<ArenaAccount>&, const char*& why) {
    why = "shared areas are not available on Windows";
    return nullptr;
}

SessionArena::~SessionArena() = default;

} // namespace rpc::detail

#else

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>

#include <algorithm>

namespace rpc::detail {


// ---- ClientArena ------------------------------------------------------------------------------------------------

std::shared_ptr<ClientArena> ClientArena::create(uint64_t bytes, const DeviceRegistration& registration) {
    bytes = (bytes + kArenaPage - 1) / kArenaPage * kArenaPage;
    if (bytes == 0)
        bytes = kArenaPage;
    std::shared_ptr<ClientArena> arena(new ClientArena());
    arena->memory_ = bufs::shm::Memory::create("librpc-arena", static_cast<size_t>(bytes));
    arena->alloc_ = std::make_unique<mem::PageClassAllocator>(bytes);
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

#endif
