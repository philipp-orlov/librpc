// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

#include "rpc/memory.hpp"

#include <algorithm>
#include <cstdlib>
#include <new>

namespace rpc {

namespace {
constexpr size_t kPage = 4096;

size_t classOf(size_t bytes, size_t minShift) {
    size_t shift = minShift;
    while ((size_t{1} << shift) < bytes)
        ++shift;
    return shift - minShift;
}
} // namespace

PinnedPool::PinnedPool(DeviceRegistration registration) : registration_(registration) {}

PinnedPool::~PinnedPool() {
    for (const BlockInfo& block : all_) {
        void* host = reinterpret_cast<void*>(block.host);
        if (block.device && registration_.unregisterMemory)
            registration_.unregisterMemory(registration_.ctx, host, block.bytes);
        std::free(host);
    }
}

InboundMemory PinnedPool::memory() noexcept {
    InboundMemory m;
    m.allocate = [](void* self, size_t bytes) { return static_cast<PinnedPool*>(self)->take(bytes); };
    m.release = [](void* self, void* data, size_t bytes) { static_cast<PinnedPool*>(self)->give(data, bytes); };
    m.device = [](void* self, const void* host) { return static_cast<PinnedPool*>(self)->device(host); };
    m.ctx = this;
    return m;
}

const void* PinnedPool::device(const void* host) const {
    const uintptr_t address = reinterpret_cast<uintptr_t>(host);
    std::lock_guard<std::mutex> lock(mutex_);
    auto after = std::upper_bound(all_.begin(), all_.end(), address, [](uintptr_t a, const BlockInfo& b) { return a < b.host; });
    if (after == all_.begin())
        return nullptr;
    const BlockInfo& block = *(after - 1);
    if (address - block.host >= block.bytes || !block.device)
        return nullptr;
    return static_cast<const uint8_t*>(block.device) + (address - block.host);
}

void* PinnedPool::take(size_t bytes) {
    if (bytes == 0)
        bytes = 1;
    const size_t cls = classOf(bytes, kMinShift);
    if (cls >= kClasses)
        return nullptr;
    const size_t size = size_t{1} << (cls + kMinShift);
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!free_[cls].empty()) {
            void* p = free_[cls].back();
            free_[cls].pop_back();
            inUse_.fetch_add(1, std::memory_order_relaxed);
            return p;
        }
    }
    // Grows: a new block, registered once. The system calls run outside the lock.
    void* p = std::aligned_alloc(kPage, size);
    if (!p)
        return nullptr;
    void* device = nullptr;
    if (registration_) {
        device = registration_.registerMemory(registration_.ctx, p, size);
        if (!device) {
            std::free(p);
            return nullptr;
        }
    }
    {
        std::lock_guard<std::mutex> lock(mutex_);
        const BlockInfo block{reinterpret_cast<uintptr_t>(p), size, device};
        all_.insert(std::upper_bound(all_.begin(), all_.end(), block.host, [](uintptr_t a, const BlockInfo& b) { return a < b.host; }), block);
    }
    inUse_.fetch_add(1, std::memory_order_relaxed);
    return p;
}

void PinnedPool::give(void* data, size_t bytes) {
    if (bytes == 0)
        bytes = 1;
    const size_t cls = classOf(bytes, kMinShift);
    {
        std::lock_guard<std::mutex> lock(mutex_);
        free_[cls].push_back(data);
    }
    inUse_.fetch_sub(1, std::memory_order_relaxed);
}

size_t PinnedPool::blocks() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return all_.size();
}

size_t PinnedPool::bytes() const {
    std::lock_guard<std::mutex> lock(mutex_);
    size_t total = 0;
    for (const BlockInfo& block : all_)
        total += block.bytes;
    return total;
}

} // namespace rpc
