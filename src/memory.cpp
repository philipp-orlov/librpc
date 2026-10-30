// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

#include "rpc/memory.hpp"

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

PinnedPool::PinnedPool(Registration registration) : registration_(registration) {}

PinnedPool::~PinnedPool() {
    for (size_t i = 0; i < all_.size(); ++i) {
        if (registration_.unregisterMemory)
            registration_.unregisterMemory(registration_.ctx, all_[i], allSizes_[i]);
        std::free(all_[i]);
    }
}

InboundMemory PinnedPool::memory() noexcept {
    InboundMemory m;
    m.allocate = [](void* self, size_t bytes) { return static_cast<PinnedPool*>(self)->take(bytes); };
    m.release = [](void* self, void* data, size_t bytes) { static_cast<PinnedPool*>(self)->give(data, bytes); };
    m.ctx = this;
    return m;
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
    if (registration_.registerMemory && !registration_.registerMemory(registration_.ctx, p, size)) {
        std::free(p);
        return nullptr;
    }
    {
        std::lock_guard<std::mutex> lock(mutex_);
        all_.push_back(p);
        allSizes_.push_back(size);
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
    for (size_t s : allSizes_)
        total += s;
    return total;
}

} // namespace rpc
