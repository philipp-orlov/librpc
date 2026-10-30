// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <utility>

#include "bufs/pool.hpp"
#include "rpc/memory.hpp"

namespace rpc::detail {

/// A received frame's memory: a block of bufs::BufferPool, or one the caller's InboundMemory supplied.
/// Move-only; the block goes back where it came from when this is destroyed.
class Block {
public:
    Block() = default;
    Block(bufs::Buffer pooled) noexcept : pooled_(std::move(pooled)) {}  // NOLINT: implicit on purpose
    Block(const InboundMemory& memory, void* data, size_t bytes) noexcept : external_(memory), data_(static_cast<uint8_t*>(data)), bytes_(bytes) {}
    ~Block() { reset(); }
    Block(Block&& other) noexcept
        : pooled_(std::move(other.pooled_)), external_(other.external_), data_(std::exchange(other.data_, nullptr)),
          bytes_(std::exchange(other.bytes_, 0)) {}
    Block& operator=(Block&& other) noexcept {
        if (this != &other) {
            reset();
            pooled_ = std::move(other.pooled_);
            external_ = other.external_;
            data_ = std::exchange(other.data_, nullptr);
            bytes_ = std::exchange(other.bytes_, 0);
        }
        return *this;
    }
    Block(const Block&) = delete;
    Block& operator=(const Block&) = delete;

    uint8_t* data() const noexcept { return data_ ? data_ : pooled_.data(); }
    explicit operator bool() const noexcept { return data_ != nullptr || static_cast<bool>(pooled_); }

    void reset() noexcept {
        if (data_)
            external_.release(external_.ctx, data_, bytes_);
        data_ = nullptr;
        bytes_ = 0;
        pooled_.reset();
    }

private:
    bufs::Buffer pooled_;
    InboundMemory external_;
    uint8_t* data_ = nullptr;  // set: the block is the caller's
    size_t bytes_ = 0;
};

/// `size` bytes copied into 8-aligned memory: the caller's when `memory` provides it, else the pooled block.
inline Block copyInbound(const InboundMemory& memory, const uint8_t* data, size_t size) {
    if (memory) {
        void* block = nullptr;
        try {
            block = memory.allocate(memory.ctx, size);
        } catch (...) {
        }
        if (block) {
            if (size)
                std::memcpy(block, data, size);
            return Block(memory, block, size);
        }
    }
    bufs::Buffer buffer = bufs::BufferPool::global().acquire(size);
    if (size)
        std::memcpy(buffer.data(), data, size);
    return Block(std::move(buffer));
}

} // namespace rpc::detail
