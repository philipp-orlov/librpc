// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// parseFrame takes bytes straight from a socket or a shared-memory ring. Whatever it accepts must be readable
// without leaving the frame: the envelope's strings are touched and the body header re-read. The input is copied
// into an exact-size, 8-aligned allocation so AddressSanitizer reports any access past its end.

#include <cstdint>
#include <cstring>
#include <new>
#include <string>
#include <vector>

#include "rpc/detail/frame.hpp"

namespace {
volatile uint64_t g_sink;
}

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    uint8_t* frame = new (std::align_val_t(8)) uint8_t[size ? size : 1];
    if (size)
        std::memcpy(frame, data, size);
    rpc::detail::Frame parsed;
    if (rpc::detail::parseFrame(frame, size, parsed)) {
        g_sink += parsed.envelope.message().size() + parsed.envelope.peer().size() + parsed.envelope.token().size();
        g_sink += parsed.envelope.id() + static_cast<uint64_t>(parsed.envelope.kind());
        if (parsed.bodySize) {
            g_sink += bufs::messageSize(parsed.bodyData) + bufs::messageType(parsed.bodyData);
            g_sink += parsed.bodyData[parsed.bodySize - 1];
        }
    }
    ::operator delete[](frame, std::align_val_t(8));
    return 0;
}

std::vector<std::string> fuzzSeeds() {
    std::vector<std::string> seeds;
    auto [env, envSize] = rpc::detail::envelope([](rpc::wire::Envelope::Builder& e) {
        e.setId(3);
        e.setKind(rpc::wire::Kind::Call);
        e.setMessage("hello");
    });
    std::string frame(reinterpret_cast<const char*>(env), envSize);
    seeds.push_back(frame);
    std::string body(16, '\0');  // a body is a message of its own: size, type id, then payload
    const uint32_t bodySize = 16;
    std::memcpy(&body[0], &bodySize, sizeof bodySize);
    seeds.push_back(frame + body);
    return seeds;
}
