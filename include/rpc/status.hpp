// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// librpc -- how a call ends: a Code and a message, as a value (Status) or as an exception (Error).
#pragma once

#include <cstdint>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

namespace rpc {

/// The numbers are the wire's (Envelope.code) and match gRPC's, so a gateway can map them one to one.
enum class Code : uint16_t {
    Ok = 0,
    Cancelled = 1,
    Unknown = 2,
    InvalidArgument = 3,
    DeadlineExceeded = 4,
    NotFound = 5,
    AlreadyExists = 6,
    PermissionDenied = 7,
    ResourceExhausted = 8,
    FailedPrecondition = 9,
    Aborted = 10,
    OutOfRange = 11,
    Unimplemented = 12,
    Internal = 13,
    Unavailable = 14,
    DataLoss = 15,
    Unauthenticated = 16,
};

const char* codeName(Code code) noexcept;

class Status {
public:
    Status() = default;
    Status(Code code, std::string message = {}, uint32_t detail = 0)
        : code_(code), message_(std::move(message)), detail_(detail) {}

    static Status ok() { return {}; }

    bool isOk() const noexcept { return code_ == Code::Ok; }
    explicit operator bool() const noexcept { return isOk(); }
    Code code() const noexcept { return code_; }
    const std::string& message() const noexcept { return message_; }
    /// Application-defined, travels with the status; 0 = none. An application that maps its own error type onto
    /// Code (libsql: its ErrorCode and Constraint) puts the exact value here.
    uint32_t detail() const noexcept { return detail_; }
    /// "NotFound: no such user"
    std::string toString() const;

private:
    Code code_ = Code::Ok;
    std::string message_;
    uint32_t detail_ = 0;
};

/// What a client call throws when it does not end in Ok, and what a handler may throw to end its call
/// with that status. Any other exception from a handler ends the call with Code::Internal.
class Error : public std::runtime_error {
public:
    explicit Error(Status status) : std::runtime_error(status.toString()), status_(std::move(status)) {}
    Error(Code code, std::string message, uint32_t detail = 0) : Error(Status(code, std::move(message), detail)) {}

    const Status& status() const noexcept { return status_; }
    Code code() const noexcept { return status_.code(); }

private:
    Status status_;
};

/// FNV-1a (32 bit) of "namespace.Service.Method": the number a Call frame carries. The compiler
/// checks that no two methods of a schema share one.
constexpr uint32_t methodId(std::string_view qualifiedName) noexcept {
    uint32_t hash = 2166136261u;
    for (char c : qualifiedName) {
        hash ^= static_cast<uint8_t>(c);
        hash *= 16777619u;
    }
    return hash;
}

} // namespace rpc
