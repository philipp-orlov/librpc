// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

#include "stream_socket.hpp"

#include <afunix.h>
#include <io.h>
#include <sddl.h>

#include <algorithm>
#include <chrono>
#include <cstring>
#include <memory>
#include <stdexcept>

#pragma comment(lib, "advapi32.lib")

namespace rpc::detail {

namespace {

std::string socketError(const std::string& what, int error = sys::lastSocketError()) {
    char* text = nullptr;
    const DWORD n = ::FormatMessageA(FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
                                     nullptr, static_cast<DWORD>(error), 0, reinterpret_cast<char*>(&text), 0, nullptr);
    std::string message = what + ": ";
    if (n > 0 && text) {
        std::string detail(text, n);
        while (!detail.empty() && (detail.back() == '\n' || detail.back() == '\r' || detail.back() == ' '))
            detail.pop_back();
        message += detail;
    } else {
        message += "error " + std::to_string(error);
    }
    if (text)
        ::LocalFree(text);
    return message;
}

void networking() {
    static const sys::Pal pal;  // WSAStartup, once for the process
    (void)pal;
}

sockaddr_un addressOf(const std::string& path) {
    sockaddr_un address{};
    address.sun_family = AF_UNIX;
    if (path.empty() || path.size() >= sizeof address.sun_path)
        throw std::invalid_argument("a socket path is 1 to " + std::to_string(sizeof address.sun_path - 1) + " bytes");
    std::memcpy(address.sun_path, path.data(), path.size());
    return address;
}

sys::socket_t newSocket() {
    networking();
    const sys::socket_t handle = ::socket(AF_UNIX, SOCK_STREAM, 0);
    if (handle == sys::kInvalidSocket)
        throw std::runtime_error(socketError("socket"));
    // Not inherited by a program the process starts (a session of the tunnel agent, say).
    ::SetHandleInformation(reinterpret_cast<HANDLE>(handle), HANDLE_FLAG_INHERIT, 0);
    return handle;
}

void setBlocking(sys::socket_t handle, bool blocking) {
    u_long nonBlocking = blocking ? 0 : 1;
    ::ioctlsocket(handle, FIONBIO, &nonBlocking);
}

bool pathExists(const std::string& path) { return ::GetFileAttributesA(path.c_str()) != INVALID_FILE_ATTRIBUTES; }

// ---- identity -----------------------------------------------------------------------------------------------------

class Token {
public:
    Token() = default;
    explicit Token(HANDLE handle) : handle_(handle) {}
    ~Token() {
        if (handle_)
            ::CloseHandle(handle_);
    }
    Token(const Token&) = delete;
    Token& operator=(const Token&) = delete;
    HANDLE get() const { return handle_; }
    explicit operator bool() const { return handle_ != nullptr; }

private:
    HANDLE handle_ = nullptr;
};

template <class T>
std::unique_ptr<char[]> tokenInformation(HANDLE token, TOKEN_INFORMATION_CLASS kind) {
    DWORD size = 0;
    ::GetTokenInformation(token, kind, nullptr, 0, &size);
    if (size == 0)
        return nullptr;
    std::unique_ptr<char[]> buffer(new char[size]);
    if (!::GetTokenInformation(token, kind, buffer.get(), size, &size))
        return nullptr;
    return buffer;
}

// This process's account, read once.
const std::unique_ptr<char[]>& ownUser() {
    static const std::unique_ptr<char[]> user = [] {
        HANDLE raw = nullptr;
        if (!::OpenProcessToken(::GetCurrentProcess(), TOKEN_QUERY, &raw))
            return std::unique_ptr<char[]>();
        Token token(raw);
        return tokenInformation<TOKEN_USER>(token.get(), TokenUser);
    }();
    return user;
}

bool isOwner(DWORD pid) {
    HANDLE process = ::OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, FALSE, pid);
    if (!process)
        return false;  // another account's process, or one that is gone
    HANDLE raw = nullptr;
    const BOOL opened = ::OpenProcessToken(process, TOKEN_QUERY, &raw);
    ::CloseHandle(process);
    if (!opened)
        return false;
    Token token(raw);

    const auto theirs = tokenInformation<TOKEN_USER>(token.get(), TokenUser);
    const auto& ours = ownUser();
    if (theirs && ours &&
        ::EqualSid(reinterpret_cast<TOKEN_USER*>(theirs.get())->User.Sid, reinterpret_cast<TOKEN_USER*>(ours.get())->User.Sid))
        return true;

    // An administrator who elevated: what the account that runs a service looks like to the person administering it.
    const auto elevation = tokenInformation<TOKEN_ELEVATION>(token.get(), TokenElevation);
    if (!elevation || !reinterpret_cast<TOKEN_ELEVATION*>(elevation.get())->TokenIsElevated)
        return false;
    SID_IDENTIFIER_AUTHORITY ntAuthority = SECURITY_NT_AUTHORITY;
    PSID administrators = nullptr;
    if (!::AllocateAndInitializeSid(&ntAuthority, 2, SECURITY_BUILTIN_DOMAIN_RID, DOMAIN_ALIAS_RID_ADMINS, 0, 0, 0, 0, 0, 0,
                                    &administrators))
        return false;
    bool member = false;
    if (const auto groups = tokenInformation<TOKEN_GROUPS>(token.get(), TokenGroups)) {
        const auto* list = reinterpret_cast<TOKEN_GROUPS*>(groups.get());
        for (DWORD i = 0; i < list->GroupCount && !member; ++i)
            member = (list->Groups[i].Attributes & SE_GROUP_ENABLED) && ::EqualSid(list->Groups[i].Sid, administrators);
    }
    ::FreeSid(administrators);
    return member;
}

} // namespace

// ---- StreamSocket -------------------------------------------------------------------------------------------------

void StreamSocket::reset() noexcept {
    if (handle_ != sys::kInvalidSocket) {
        sys::closeSocket(handle_);
        handle_ = sys::kInvalidSocket;
    }
}

StreamSocket StreamSocket::connect(const std::string& path, int timeoutMillis) {
    StreamSocket socket(newSocket());
    const sockaddr_un address = addressOf(path);
    setBlocking(socket.handle_, false);
    int result = ::connect(socket.handle_, reinterpret_cast<const sockaddr*>(&address), sizeof address);
    if (result != 0 && sys::lastSocketError() == WSAEWOULDBLOCK) {
        WSAPOLLFD waiting{};
        waiting.fd = socket.handle_;
        waiting.events = POLLWRNORM;
        if (::WSAPoll(&waiting, 1, timeoutMillis) != 1)
            throw std::runtime_error("cannot connect to " + path + ": timed out");
        int error = 0;
        int length = sizeof error;
        ::getsockopt(socket.handle_, SOL_SOCKET, SO_ERROR, reinterpret_cast<char*>(&error), &length);
        if (error != 0)
            throw std::runtime_error(socketError("cannot connect to " + path, error));
        result = 0;
    }
    if (result != 0)
        throw std::runtime_error(socketError("cannot connect to " + path));
    setBlocking(socket.handle_, true);
    return socket;
}

StreamSocket StreamSocket::listen(const std::string& path) {
    const sockaddr_un address = addressOf(path);
    if (pathExists(path)) {
        // A file left by a process that died is replaced; a live listener is not.
        bool alive = false;
        try {
            StreamSocket probe = connect(path, 500);
            alive = true;
        } catch (const std::runtime_error&) {
        }
        if (alive)
            throw std::runtime_error("another process is listening on " + path);
        if (!removeSocketFile(path))
            throw std::runtime_error("cannot replace the old socket file " + path);
    }
    StreamSocket listener(newSocket());
    if (::bind(listener.handle_, reinterpret_cast<const sockaddr*>(&address), sizeof address) != 0)
        throw std::runtime_error(socketError("cannot listen on " + path));
    if (::listen(listener.handle_, 64) != 0) {
        const std::string message = socketError("cannot listen on " + path);
        removeSocketFile(path);
        throw std::runtime_error(message);
    }
    setBlocking(listener.handle_, false);
    return listener;
}

StreamSocket StreamSocket::accept() const {
    const sys::socket_t handle = ::accept(handle_, nullptr, nullptr);
    if (handle == sys::kInvalidSocket) {
        const int error = sys::lastSocketError();
        if (error != WSAEWOULDBLOCK && error != WSAECONNRESET && error != WSAEINTR)
            throw std::runtime_error(socketError("accept", error));
        return StreamSocket();
    }
    ::SetHandleInformation(reinterpret_cast<HANDLE>(handle), HANDLE_FLAG_INHERIT, 0);
    setBlocking(handle, true);
    return StreamSocket(handle);
}

bool StreamSocket::waitReadable(int millis) const {
    WSAPOLLFD waiting{};
    waiting.fd = handle_;
    waiting.events = POLLRDNORM;
    return ::WSAPoll(&waiting, 1, millis) > 0;
}

void StreamSocket::sendAll(const void* data, size_t size) const {
    const char* bytes = static_cast<const char*>(data);
    while (size > 0) {
        const int sent = ::send(handle_, bytes, static_cast<int>(std::min<size_t>(size, 1u << 20)), 0);
        if (sent <= 0) {
            const int error = sys::lastSocketError();
            if (error == WSAEINTR)
                continue;
            throw std::runtime_error(socketError(error == WSAETIMEDOUT ? "the peer is not reading" : "send", error));
        }
        bytes += sent;
        size -= static_cast<size_t>(sent);
    }
}

void StreamSocket::setSendTimeout(int millis) const {
    const DWORD value = static_cast<DWORD>(millis);
    ::setsockopt(handle_, SOL_SOCKET, SO_SNDTIMEO, reinterpret_cast<const char*>(&value), sizeof value);
}

long StreamSocket::receiveSome(void* data, size_t size) const {
    for (;;) {
        const int got = ::recv(handle_, static_cast<char*>(data), static_cast<int>(std::min<size_t>(size, 1u << 20)), 0);
        if (got >= 0)
            return got;
        const int error = sys::lastSocketError();
        if (error == WSAEINTR)
            continue;
        if (error == WSAECONNRESET || error == WSAECONNABORTED || error == WSAESHUTDOWN || error == WSAENOTSOCK)
            return 0;  // as good as the end of the stream
        throw std::runtime_error(socketError("recv", error));
    }
}

bool StreamSocket::receiveExactly(void* data, size_t size, int timeoutMillis) const {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMillis);
    char* bytes = static_cast<char*>(data);
    while (size > 0) {
        const auto left = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - std::chrono::steady_clock::now()).count();
        if (left <= 0 || !waitReadable(static_cast<int>(left)))
            return false;
        const long got = receiveSome(bytes, size);
        if (got == 0)
            return false;
        bytes += got;
        size -= static_cast<size_t>(got);
    }
    return true;
}

PeerIdentity StreamSocket::peer() const {
    PeerIdentity identity;
    ULONG pid = 0;
    DWORD returned = 0;
    if (::WSAIoctl(handle_, SIO_AF_UNIX_GETPEERPID, nullptr, 0, &pid, sizeof pid, &returned, nullptr, nullptr) == 0 && pid != 0) {
        identity.pid = pid;
        identity.owner = isOwner(pid);
    }
    return identity;
}

void StreamSocket::shutdown() const {
    if (handle_ != sys::kInvalidSocket)
        ::shutdown(handle_, SD_BOTH);
}

bool removeSocketFile(const std::string& path) noexcept { return ::DeleteFileA(path.c_str()) != 0 || !pathExists(path); }

// ---- frames -------------------------------------------------------------------------------------------------------

FrameReader::Result FrameReader::read(const StreamSocket& socket, int timeoutMillis, std::vector<uint8_t>& frame, size_t maxFrame) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMillis);
    for (;;) {
        const size_t available = buffer_.size() - position_;
        if (available >= 4) {
            uint32_t length;
            std::memcpy(&length, buffer_.data() + position_, 4);
            if (length > maxFrame)
                throw std::runtime_error("a frame of " + std::to_string(length) + " bytes exceeds the limit of " + std::to_string(maxFrame));
            if (available >= 4 + static_cast<size_t>(length)) {
                frame.assign(buffer_.begin() + static_cast<std::ptrdiff_t>(position_ + 4),
                             buffer_.begin() + static_cast<std::ptrdiff_t>(position_ + 4 + length));
                position_ += 4 + length;
                if (position_ == buffer_.size()) {
                    buffer_.clear();
                    position_ = 0;
                } else if (position_ > (1u << 20)) {
                    buffer_.erase(buffer_.begin(), buffer_.begin() + static_cast<std::ptrdiff_t>(position_));
                    position_ = 0;
                }
                return Result::Frame;
            }
        }
        const auto left = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - std::chrono::steady_clock::now()).count();
        if (!socket.waitReadable(static_cast<int>(std::max<long long>(left, 0))))
            return Result::Timeout;
        uint8_t chunk[64 * 1024];
        const long got = socket.receiveSome(chunk, sizeof chunk);
        if (got == 0)
            return Result::Closed;
        buffer_.insert(buffer_.end(), chunk, chunk + got);
    }
}

void sendFrame(const StreamSocket& socket, std::vector<uint8_t>& scratch, const uint8_t* envelope, size_t envelopeSize,
               const uint8_t* body, size_t bodySize) {
    const size_t total = envelopeSize + bodySize;
    if (total > 0xFFFFFFFFull)
        throw std::runtime_error("a frame larger than 4 GiB");
    scratch.resize(4 + total);
    const uint32_t length = static_cast<uint32_t>(total);
    std::memcpy(scratch.data(), &length, 4);
    std::memcpy(scratch.data() + 4, envelope, envelopeSize);
    if (bodySize)
        std::memcpy(scratch.data() + 4 + envelopeSize, body, bodySize);
    socket.sendAll(scratch.data(), scratch.size());
    if (scratch.capacity() > (4u << 20) && total < (1u << 16))
        std::vector<uint8_t>().swap(scratch);  // one huge frame must not pin its memory for ever
}

} // namespace rpc::detail
