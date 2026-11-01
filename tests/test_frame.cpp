// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// parseFrame must reject a frame whose tail is too short to hold a body header without reading past the
// end. Every frame is copied into an exact-size allocation, so a sanitizer reports any over-read.
#include <cstdio>
#include <cstring>
#include <new>
#include <vector>

#include "rpc/detail/frame.hpp"

namespace {
int g_failures = 0;

#define CHECK(cond)                                                          \
    do {                                                                     \
        if (!(cond)) {                                                       \
            std::fprintf(stderr, "%s:%d: CHECK(%s) failed\n", __FILE__, __LINE__, #cond); \
            ++g_failures;                                                    \
        }                                                                    \
    } while (0)

struct Exact {
    explicit Exact(const std::vector<uint8_t>& bytes)
        : data(new (std::align_val_t(8)) uint8_t[bytes.size()]), size(bytes.size()) {
        std::memcpy(data, bytes.data(), bytes.size());
    }
    ~Exact() { ::operator delete[](data, std::align_val_t(8)); }
    uint8_t* data;
    size_t size;
};
}  // namespace

int main() {
    using namespace rpc;
    auto [env, envSize] = detail::envelope([](wire::Envelope::Builder& e) { e.setId(7); });
    std::vector<uint8_t> frame(env, env + envSize);

    {  // an envelope alone
        Exact f(frame);
        detail::Frame out;
        CHECK(detail::parseFrame(f.data, f.size, out));
        CHECK(out.bodySize == 0);
    }
    for (size_t extra : {1, 2, 3, 4, 5, 6, 7}) {  // fewer than kHeaderBytes stray bytes
        std::vector<uint8_t> bad = frame;
        bad.insert(bad.end(), extra, 0);
        Exact f(bad);
        detail::Frame out;
        CHECK(!detail::parseFrame(f.data, f.size, out));
    }
    {  // a body that declares more than is there
        std::vector<uint8_t> bad = frame;
        bad.insert(bad.end(), 8, 0);
        const uint32_t huge = 4096;
        std::memcpy(bad.data() + frame.size(), &huge, sizeof huge);
        Exact f(bad);
        detail::Frame out;
        CHECK(!detail::parseFrame(f.data, f.size, out));
    }
    if (g_failures == 0)
        std::puts("ok");
    return g_failures == 0 ? 0 : 1;
}
