// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

#include "rpc/memory.hpp"

namespace rpc {

PinnedPool::PinnedPool(DeviceRegistration registration)
    : registration_(registration),
      pool_(registration ? mem::Registration{&registerBlock, &unregisterBlock, this} : mem::Registration{}) {}

PinnedPool::PinnedPool(DeviceRegistration registration, Limits limits)
    : registration_(registration),
      pool_(registration ? mem::Registration{&registerBlock, &unregisterBlock, this} : mem::Registration{}, convert(limits)) {}

PinnedPool::~PinnedPool() = default;

InboundMemory PinnedPool::memory() noexcept {
    InboundMemory m;
    m.allocate = [](void* self, size_t bytes) { return static_cast<PinnedPool*>(self)->pool_.take(bytes); };
    m.release = [](void* self, void* data, size_t bytes) { static_cast<PinnedPool*>(self)->pool_.give(data, bytes); };
    m.device = [](void* self, const void* host) { return static_cast<PinnedPool*>(self)->pool_.device(host); };
    m.ctx = this;
    return m;
}

const void* PinnedPool::device(const void* host) const { return pool_.device(host); }

size_t PinnedPool::blocks() const { return pool_.stats().blocks; }
size_t PinnedPool::bytes() const { return pool_.stats().bytes; }
size_t PinnedPool::trim(size_t keepIdleBytes) { return pool_.trim(keepIdleBytes); }

PinnedPool::Stats PinnedPool::stats() const {
    const mem::RegisteredPool::Stats s = pool_.stats();
    Stats out;
    out.blocks = s.blocks;
    out.bytes = s.bytes;
    out.inUseBlocks = s.inUseBlocks;
    out.idleBytes = s.idleBytes;
    out.peakBytes = s.peakBytes;
    out.refusals = s.refusals;
    out.evictedBlocks = s.evictedBlocks;
    return out;
}

} // namespace rpc
