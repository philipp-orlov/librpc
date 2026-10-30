// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

#include "rpc/status.hpp"

namespace rpc {

const char* codeName(Code code) noexcept {
    switch (code) {
    case Code::Ok: return "Ok";
    case Code::Cancelled: return "Cancelled";
    case Code::Unknown: return "Unknown";
    case Code::InvalidArgument: return "InvalidArgument";
    case Code::DeadlineExceeded: return "DeadlineExceeded";
    case Code::NotFound: return "NotFound";
    case Code::AlreadyExists: return "AlreadyExists";
    case Code::PermissionDenied: return "PermissionDenied";
    case Code::ResourceExhausted: return "ResourceExhausted";
    case Code::FailedPrecondition: return "FailedPrecondition";
    case Code::Aborted: return "Aborted";
    case Code::OutOfRange: return "OutOfRange";
    case Code::Unimplemented: return "Unimplemented";
    case Code::Internal: return "Internal";
    case Code::Unavailable: return "Unavailable";
    case Code::DataLoss: return "DataLoss";
    case Code::Unauthenticated: return "Unauthenticated";
    }
    return "Unknown";
}

std::string Status::toString() const {
    std::string out = codeName(code_);
    if (!message_.empty()) {
        out += ": ";
        out += message_;
    }
    return out;
}

} // namespace rpc
