// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// rpc::Span<T>: a pointer and a count. How the generated stubs take and return the vector parameters of
// natural-form methods without a std::vector (and so without a heap allocation) on either side.
#pragma once

#include <array>
#include <cstddef>
#include <type_traits>

namespace rpc {

template <class T>
class Span {
public:
    constexpr Span() noexcept = default;
    constexpr Span(T* data, size_t size) noexcept : data_(data), size_(size) {}
    template <size_t N>
    constexpr Span(T (&array)[N]) noexcept : data_(array), size_(N) {}
    /// From std::vector, std::array, bufs::Vector or anything else contiguous with data() and size().
    template <class C, class = std::enable_if_t<std::is_convertible_v<decltype(std::declval<const C&>().data()), T*>>>
    constexpr Span(const C& container) noexcept : data_(container.data()), size_(container.size()) {}

    constexpr T* data() const noexcept { return data_; }
    constexpr size_t size() const noexcept { return size_; }
    constexpr bool empty() const noexcept { return size_ == 0; }
    constexpr T& operator[](size_t i) const noexcept { return data_[i]; }
    constexpr T* begin() const noexcept { return data_; }
    constexpr T* end() const noexcept { return data_ + size_; }

private:
    T* data_ = nullptr;
    size_t size_ = 0;
};

} // namespace rpc
