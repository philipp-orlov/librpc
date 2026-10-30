// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

#include "rpc/client.hpp"

#include <poll.h>

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstring>
#include <mutex>
#include <optional>
#include <thread>

#include "bufs/local.hpp"
#include "bufs/shm.hpp"
#include "http/websocket_client.hpp"
#include "ipc_common.hpp"
#include "rpc/detail/frame.hpp"
#include "rpc/detail/link.hpp"

namespace rpc {

using Clock = std::chrono::steady_clock;

namespace detail {

// ---- links ----------------------------------------------------------------------------------------------------

class WebSocketClientLink final : public ClientLink {
public:
    WebSocketClientLink(std::string_view url, const ClientOptions& options, size_t maxMessage) : pingEvery_(options.pingInterval) {
        http::WebSocketClientOptions wsOptions;
        wsOptions.headers = options.headers;
        wsOptions.subprotocols = {kSubprotocol};
        wsOptions.connectTimeout = options.connectTimeout;
        wsOptions.maxMessageBytes = maxMessage;
        wsOptions.caFile = options.caFile;
        wsOptions.insecureSkipVerify = options.insecureSkipVerify;
        try {
            ws_.connect(url, wsOptions);
        } catch (const http::WebSocketError& error) {
            if (error.kind() == http::WebSocketError::Kind::Handshake) {
                Code code = Code::Unavailable;
                if (error.status() == 401)
                    code = Code::Unauthenticated;
                else if (error.status() == 403)
                    code = Code::PermissionDenied;
                else if (error.status() == 404)
                    code = Code::NotFound;
                throw Error(code, error.what());
            }
            throw Error(Code::Unavailable, error.what());
        }
    }

    void send(const uint8_t* envelope, size_t envelopeSize, const uint8_t* body, size_t bodySize) override {
        bufs::Buffer frame = bufs::BufferPool::global().acquire(envelopeSize + bodySize);
        std::memcpy(frame.data(), envelope, envelopeSize);
        if (bodySize)
            std::memcpy(frame.data() + envelopeSize, body, bodySize);
        try {
            ws_.sendBinary(frame.data(), envelopeSize + bodySize);
        } catch (const http::WebSocketError& error) {
            throw LinkError(error.what());
        }
    }

    bool receive(Inbound& frame, Clock::time_point deadline) override {
        try {
            auto message = ws_.receive(deadline, pingEvery_);
            if (!message)
                return false;
            if (!message->binary)
                throw LinkError("text frame from server");
            frame.buffer = alignedCopy(reinterpret_cast<const uint8_t*>(message->data.data()), message->data.size());
            frame.size = message->data.size();
            return true;
        } catch (const http::WebSocketError& error) {
            throw LinkError(error.what());
        }
    }

    void close() noexcept override { ws_.close(); }  // wakes a thread blocked in receive()

private:
    http::WebSocketClient ws_;
    std::chrono::milliseconds pingEvery_;
};

class LocalClientLink final : public ClientLink {
public:
    LocalClientLink(const std::string& path, const ClientOptions& options)
        : spin_(std::chrono::duration_cast<std::chrono::nanoseconds>(options.ipcSpin)) {
        try {
            socket_ = bufs::local::Socket::connect(path);
            IpcHello hello;
            bufs::local::ReceivedFds fds;
            // Blocks until the server's handshake arrives; a server that never sends it is a dead one.
            pollfd p{socket_.fd(), POLLIN, 0};
            if (::poll(&p, 1, static_cast<int>(options.connectTimeout.count())) <= 0)
                throw Error(Code::Unavailable, "no handshake from " + path);
            const long got = socket_.receive(&hello, sizeof hello, fds);
            if (got != static_cast<long>(sizeof hello) || hello.magic != IpcHello::kMagic || hello.version != 1)
                throw Error(Code::Unavailable, path + " is not a librpc socket");
            if (hello.status != 0)
                throw Error(hello.status == 401 ? Code::Unauthenticated : hello.status == 403 ? Code::PermissionDenied : Code::Unavailable,
                            path + " refused the connection (" + std::to_string(hello.status) + ")");
            if (fds.count != 1)
                throw Error(Code::Unavailable, path + " sent no channel");
            channel_ = bufs::shm::Channel::adopt(fds.take(0), 1ull << 32);
        } catch (const std::system_error& error) {
            throw Error(error.code() == std::errc::permission_denied ? Code::PermissionDenied : Code::Unavailable,
                        std::string("cannot connect to ") + path + ": " + error.what());
        } catch (const Error&) {
            throw;
        } catch (const std::exception& error) {
            throw Error(Code::Unavailable, std::string("cannot connect to ") + path + ": " + error.what());
        }
    }

    void send(const uint8_t* envelope, size_t envelopeSize, const uint8_t* body, size_t bodySize) override {
        const size_t total = envelopeSize + bodySize;
        std::lock_guard<std::mutex> lock(sendMutex_);
        if (closed_.load())
            throw LinkError("the connection is closed");
        try {
            // Waits for the server to free room (the stream window normally keeps the rings from filling),
            // and gives up on a server that stopped reading.
            uint8_t* slot = channel_.send().reserve(total, std::chrono::seconds(30), spin_);
            if (!slot)
                throw LinkError("the server is not reading");
            std::memcpy(slot, envelope, envelopeSize);
            if (bodySize)
                std::memcpy(slot + envelopeSize, body, bodySize);
            channel_.send().commit(total);
        } catch (const bufs::CapacityError&) {
            throw LinkError("message exceeds the ring");
        } catch (const bufs::shm::CorruptRing& error) {
            throw LinkError(error.what());
        }
    }

    bool receive(Inbound& frame, Clock::time_point deadline) override {
        bufs::shm::Ring& ring = channel_.receive();
        try {
            for (;;) {
                const uint8_t* data = nullptr;
                size_t size = 0;
                if (ring.front(data, size)) {
                    // Copied out: the server is another process and may change the bytes under us.
                    frame.buffer = alignedCopy(data, size);
                    frame.size = size;
                    ring.pop();
                    return true;
                }
                if (closed_.load())
                    throw LinkError("client closed");
                if (peerGone())
                    throw LinkError("the server went away");
                const auto now = Clock::now();
                if (now >= deadline)
                    return false;
                // Slices, so that a vanished peer or close() is noticed.
                const auto slice = std::min<Clock::duration>(deadline - now, std::chrono::milliseconds(50));
                ring.wait(std::chrono::duration_cast<std::chrono::nanoseconds>(slice), spin_);
            }
        } catch (const bufs::shm::CorruptRing& error) {
            throw LinkError(error.what());
        }
    }

    void close() noexcept override {
        closed_.store(true);
        socket_.shutdown();  // the server sees the connection end
    }

private:
    bool peerGone() {
        pollfd p{socket_.fd(), POLLRDHUP, 0};
        return ::poll(&p, 1, 0) > 0 && (p.revents & (POLLHUP | POLLERR | POLLRDHUP));
    }

    bufs::local::Socket socket_;
    bufs::shm::Channel channel_;
    std::chrono::nanoseconds spin_;
    std::mutex sendMutex_;
    std::atomic<bool> closed_{false};
};

// ---- call table -----------------------------------------------------------------------------------------------

struct Item {
    Payload payload;
    size_t frameBytes = 0;
};

// One call in progress. Slots are allocated once and reused, so a call allocates nothing once warm. Guarded by
// ClientImpl::mu.
struct CallState {
    uint32_t id = 0;  // 0: the slot is free
    bool stream = false;
    bool ended = false;
    bool waiting = false;  // a thread sleeps on cv (guarded by mu)
    bool detached = false; // Client::notify: nobody waits; the End frees the slot
    bool sendClosed = false; // the caller finished its stream
    Status status;
    Payload response;  // a unary call's response
    std::condition_variable cv;

    // The stream items not yet consumed: a ring that keeps its capacity.
    std::vector<Item> items;
    size_t head = 0;
    size_t count = 0;

    bool hasItems() const { return count != 0; }
    void push(Item&& item) {
        if (count == items.size()) {
            std::vector<Item> grown(std::max<size_t>(8, items.size() * 2));
            for (size_t i = 0; i < count; ++i)
                grown[i] = std::move(items[(head + i) % items.size()]);
            items = std::move(grown);
            head = 0;
        }
        items[(head + count) % items.size()] = std::move(item);
        ++count;
    }
    Item pop() {
        Item out = std::move(items[head]);
        head = (head + 1) % items.size();
        --count;
        return out;
    }
    // Drops what is queued; returns its frame bytes (still owed to the server as credit).
    size_t dropItems() {
        size_t bytes = 0;
        while (count)
            bytes += pop().frameBytes;
        return bytes;
    }
};

constexpr uint32_t kSlotBits = 12;
constexpr uint32_t kSlotMask = (1u << kSlotBits) - 1;

struct ClientImpl {
    std::unique_ptr<ClientLink> link;
    ClientOptions options;
    std::thread reader;

    std::mutex mu;
    std::vector<std::unique_ptr<CallState>> slots;
    std::vector<uint32_t> freeSlots;
    uint32_t sequence = 1;
    bool closed = false;
    bool stopping = false;
    Status closeStatus;
    uint64_t consumed = 0;  // frame bytes of consumed items not yet reported to the server
    uint64_t sendUnacked = 0;  // frame bytes of our stream items the server has not credited yet
    // Local connections have no reader thread: the threads that wait for replies take turns reading the ring
    // (leader/follower), so a reply is handled on the thread that wants it, without a wake-up in between.
    bool leaderMode = false;
    bool pumping = false;  // a thread is the leader right now

    std::string serverName;
    uint64_t session = 0;
    size_t window = 0;
    size_t serverMaxMessage = 0;
    size_t maxInflight = 0;

    ~ClientImpl() { close(); }

    // Ends every call, wakes the reader thread and joins it.
    void close() noexcept {
        {
            std::lock_guard<std::mutex> lock(mu);
            stopping = true;
        }
        fail(Status(Code::Unavailable, "client closed"));
        const bool joining = reader.joinable() && reader.get_id() != std::this_thread::get_id();
        if (link) {
            if (joining) {
                try {
                    send(envelope([](wire::Envelope::Builder& e) { e.setKind(wire::Kind::Goodbye); }));
                } catch (...) {
                }
            }
            link->close();
        }
        if (joining)
            reader.join();
    }

    // Everything waiting ends with `status`.
    void fail(Status status) {
        std::lock_guard<std::mutex> lock(mu);
        if (!closed) {
            closed = true;
            closeStatus = std::move(status);
        }
        for (auto& slot : slots) {
            if (slot->id != 0 && !slot->ended) {
                slot->ended = true;
                slot->status = closeStatus;
            }
            slot->cv.notify_all();
        }
    }

    void send(const std::pair<const uint8_t*, size_t>& env, const uint8_t* body = nullptr, size_t bodySize = 0) {
        link->send(env.first, env.second, body, bodySize);
    }

    void sendCredit(uint64_t bytes) {
        if (!bytes)
            return;
        try {
            send(envelope([&](wire::Envelope::Builder& e) {
                e.setKind(wire::Kind::Credit);
                e.setCredit(bytes);
            }));
        } catch (const std::exception&) {
            // the reader thread notices a dead connection
        }
    }

    // `bytes` of stream items were consumed (or dropped): tell the server once a quarter window has piled up, or at once
    // when `drained` (the caller has nothing left queued), so a sender holding a frame larger than the room left is never
    // stuck behind credit that would otherwise wait for more reads.
    void consume(size_t bytes, bool drained = false) {
        uint64_t report = 0;
        {
            std::lock_guard<std::mutex> lock(mu);
            consumed += bytes;
            if (drained || consumed >= std::max<size_t>(window / 4, 1)) {
                report = consumed;
                consumed = 0;
            }
        }
        sendCredit(report);
    }

    // ---- call table (mu held) ----

    CallState* find(uint32_t id) {
        const uint32_t slot = id & kSlotMask;
        if (id != 0 && slot < slots.size() && slots[slot]->id == id)
            return slots[slot].get();
        return nullptr;
    }

    CallState* allocate(bool stream) {
        uint32_t slot;
        if (!freeSlots.empty()) {
            slot = freeSlots.back();
            freeSlots.pop_back();
        } else {
            if (slots.size() > kSlotMask)
                throw Error(Code::ResourceExhausted, "too many calls in flight");
            slot = static_cast<uint32_t>(slots.size());
            slots.push_back(std::make_unique<CallState>());
        }
        CallState* state = slots[slot].get();
        state->id = (sequence << kSlotBits) | slot;
        sequence = (sequence + 1) & 0xFFFFF;
        if (sequence == 0)
            sequence = 1;
        state->stream = stream;
        state->detached = false;
        state->sendClosed = false;
        state->ended = false;
        state->status = Status();
        return state;
    }

    // Frees the slot; returns the frame bytes of the items it still held.
    size_t release(CallState* state) {
        const size_t dropped = state->dropItems();
        state->response.reset();
        state->status = Status();
        const uint32_t slot = state->id & kSlotMask;
        state->id = 0;
        freeSlots.push_back(slot);
        return dropped;
    }

    // Gives up on a call: tells the server unless it has already ended, frees the slot, credits what was queued.
    void cancelCall(uint32_t id, bool sendCancel = true) noexcept {
        size_t dropped = 0;
        bool needCancel = false;
        {
            std::lock_guard<std::mutex> lock(mu);
            CallState* state = find(id);
            if (!state)
                return;
            needCancel = !state->ended;
            state->ended = true;
            state->cv.notify_all();
            dropped = release(state);
        }
        if (dropped)
            consume(dropped);
        if (needCancel && sendCancel) {
            try {
                send(envelope([&](wire::Envelope::Builder& e) {
                    e.setId(id);
                    e.setKind(wire::Kind::Cancel);
                }));
            } catch (...) {
            }
        }
    }

    // ---- waiting for replies ----

    // Waits, with `lock` (mu) held, until ready() or `deadline`; false on timeout. With a reader thread this is a
    // condition-variable wait. Without one (local), a waiter that finds nobody reading becomes the reader: it
    // takes frames from the link and dispatches them (to itself or to the other waiters) until its own is done.
    template <class Ready>
    bool wait(std::unique_lock<std::mutex>& lock, CallState* state, Ready ready, Clock::time_point deadline) {
        while (!ready()) {
            if (deadline != Clock::time_point::max() && Clock::now() >= deadline) {
                handoff();
                return false;
            }
            if (leaderMode && !pumping) {
                pumping = true;
                lock.unlock();
                pumpOnce(deadline);
                lock.lock();
                pumping = false;
                continue;
            }
            state->waiting = true;
            if (deadline == Clock::time_point::max())
                state->cv.wait(lock);
            else
                state->cv.wait_until(lock, deadline);
            state->waiting = false;
        }
        handoff();
        return true;
    }

    // mu held: nobody reads now; wake one waiter to take over.
    void handoff() {
        if (!leaderMode || pumping)
            return;
        for (auto& slot : slots) {
            if (slot->waiting) {
                slot->cv.notify_one();
                return;
            }
        }
    }

    // Takes one frame from the link, waiting for it at most until `deadline` (and 50 ms: close() and a dead peer
    // are noticed in slices), and dispatches it.
    void pumpOnce(Clock::time_point deadline) {
        Inbound inbound;
        try {
            if (!link->receive(inbound, std::min(deadline, Clock::now() + std::chrono::milliseconds(50))))
                return;
        } catch (const LinkError& error) {
            bool wasStopping;
            {
                std::lock_guard<std::mutex> lock(mu);
                wasStopping = stopping;
            }
            if (!wasStopping)
                fail(Status(Code::Unavailable, std::string("connection lost: ") + error.what()));
            return;
        }
        Frame frame;
        if (!parseFrame(inbound.buffer.data(), inbound.size, frame)) {
            fail(Status(Code::DataLoss, "malformed frame from server"));
            return;
        }
        dispatch(inbound, frame);
    }

    // ---- the caller's stream (client streaming and bidirectional calls) ----

    // Sends one item of call `id`'s stream, waiting while the server has not read enough of what is already sent.
    // False when the call is over (the server answered early, or it was cancelled).
    bool sendItem(uint32_t id, const uint8_t* message, size_t size) {
        if (!size || bufs::typeOf(message, size) == 0 || (reinterpret_cast<uintptr_t>(message) & 7) != 0)
            throw Error(Code::InvalidArgument, "a stream item is not a finished, 8-aligned message");
        const size_t frameBytes = 64 + size;  // an envelope of this kind is under 64 bytes
        if (serverMaxMessage && frameBytes > serverMaxMessage + 64)
            throw Error(Code::ResourceExhausted, "stream item exceeds the server's maximum message size");
        {
            std::unique_lock<std::mutex> lock(mu);
            CallState* state = find(id);
            if (!state)
                return false;
            const auto ready = [&] {
                CallState* s = find(id);
                return !s || s->ended || closed || sendUnacked == 0 || sendUnacked + frameBytes <= window;
            };
            wait(lock, state, ready, Clock::time_point::max());
            state = find(id);
            if (!state || state->ended || closed)
                return false;
            sendUnacked += frameBytes;
        }
        try {
            const auto env = envelope([&](wire::Envelope::Builder& e) {
                e.setId(id);
                e.setKind(wire::Kind::Data);
            });
            // The server counts the whole frame it receives; ours is envelope + item, a few bytes under the 64 booked.
            const size_t actual = env.second + size;
            if (actual < frameBytes) {
                std::lock_guard<std::mutex> lock(mu);
                sendUnacked -= std::min<uint64_t>(sendUnacked, frameBytes - actual);
            }
            send(env, message, size);
        } catch (const LinkError& error) {
            throw Error(Code::Unavailable, std::string("connection lost: ") + error.what());
        }
        return true;
    }

    // The caller will send no more items.
    void sendEnd(uint32_t id) {
        {
            std::lock_guard<std::mutex> lock(mu);
            CallState* state = find(id);
            if (!state || state->ended || state->sendClosed)
                return;
            state->sendClosed = true;
        }
        try {
            send(envelope([&](wire::Envelope::Builder& e) {
                e.setId(id);
                e.setKind(wire::Kind::End);
            }));
        } catch (const LinkError&) {
            // the reader notices a dead connection
        }
    }

    // ---- reader thread ----

    void readerLoop() {
        Inbound inbound;
        for (;;) {
            try {
                if (!link->receive(inbound, Clock::time_point::max())) {
                    std::lock_guard<std::mutex> lock(mu);
                    if (stopping)
                        break;
                    continue;
                }
            } catch (const LinkError& error) {
                bool wasStopping;
                {
                    std::lock_guard<std::mutex> lock(mu);
                    wasStopping = stopping;
                }
                if (!wasStopping)
                    fail(Status(Code::Unavailable, std::string("connection lost: ") + error.what()));
                break;
            }
            Frame frame;
            if (!parseFrame(inbound.buffer.data(), inbound.size, frame)) {
                fail(Status(Code::DataLoss, "malformed frame from server"));
                break;
            }
            dispatch(inbound, frame);
        }
        link->close();
    }

    // The body of `frame` as a Payload that takes over the block the frame arrived in: no copy.
    static Payload takeBody(Inbound& inbound, const Frame& frame) {
        const size_t offset = static_cast<size_t>(frame.bodyData - inbound.buffer.data());
        return Payload(std::move(inbound.buffer), offset, frame.bodySize);
    }

    void dispatch(Inbound& inbound, const Frame& frame) {
        const wire::Envelope::Reader env = frame.envelope;
        const size_t frameBytes = inbound.size;
        switch (env.kind()) {
        case wire::Kind::Data: {
            bool queued = false;
            {
                std::lock_guard<std::mutex> lock(mu);
                CallState* state = find(env.id());
                if (state && state->stream && !state->ended && frame.bodySize) {
                    state->push(Item{takeBody(inbound, frame), frameBytes});
                    queued = true;
                    state->cv.notify_one();
                }
            }
            if (!queued)
                consume(frameBytes);  // for a call that is gone: the server still counts these bytes
            return;
        }
        case wire::Kind::End: {
            if (env.id() == 0) {  // the Hello was refused after the connection opened
                fail(Status(static_cast<Code>(env.code()), std::string(env.message()), env.detail()));
                return;
            }
            std::lock_guard<std::mutex> lock(mu);
            CallState* state = find(env.id());
            if (!state)
                return;
            if (state->detached) {  // a one-way call: the answer is not wanted
                release(state);
                return;
            }
            if (frame.bodySize)
                state->response = takeBody(inbound, frame);
            if (env.code() != 0 || env.message().size())
                state->status = Status(static_cast<Code>(env.code()), std::string(env.message()), env.detail());
            state->ended = true;
            state->cv.notify_one();
            return;
        }
        case wire::Kind::Credit: {  // the server consumed some of the items our caller streamed up
            std::lock_guard<std::mutex> lock(mu);
            sendUnacked -= std::min<uint64_t>(sendUnacked, env.credit());
            for (auto& slot : slots)
                if (slot->waiting)
                    slot->cv.notify_all();
            return;
        }
        case wire::Kind::Closing: {
            std::lock_guard<std::mutex> lock(mu);
            if (!closed)
                closeStatus = Status(Code::Unavailable, "server is shutting down");
            return;
        }
        default: return;
        }
    }

    CallState* start(uint32_t method, const uint8_t* request, size_t size, const CallOptions& callOptions, bool stream,
                     bool detached = false) {
        if (!size || bufs::typeOf(request, size) == 0 || (reinterpret_cast<uintptr_t>(request) & 7) != 0)
            throw Error(Code::InvalidArgument, "request is not a finished, 8-aligned message");
        CallState* state;
        uint32_t id;
        {
            std::lock_guard<std::mutex> lock(mu);
            if (closed)
                throw Error(closeStatus.isOk() ? Status(Code::Unavailable, "client is closed") : closeStatus);
            if (maxInflight && slots.size() - freeSlots.size() >= maxInflight)
                throw Error(Code::ResourceExhausted, "too many calls in flight on this connection");
            state = allocate(stream);
            state->detached = detached;  // before the frame goes out: the answer may come at once
            id = state->id;
        }
        try {
            const auto env = envelope([&](wire::Envelope::Builder& e) {
                e.setId(id);
                e.setKind(wire::Kind::Call);
                e.setMethod(method);
                if (callOptions.timeout.count() > 0)
                    e.setDeadlineMs(static_cast<uint32_t>(std::min<int64_t>(callOptions.timeout.count(), UINT32_MAX)));
            });
            if (serverMaxMessage && env.second + size > serverMaxMessage)
                throw Error(Code::ResourceExhausted, "request exceeds the server's maximum message size");
            send(env, request, size);
        } catch (const LinkError& error) {
            cancelCall(id, false);
            throw Error(Code::Unavailable, std::string("connection lost: ") + error.what());
        } catch (...) {
            cancelCall(id, false);
            throw;
        }
        return state;
    }
};

} // namespace detail

// --- Client -------------------------------------------------------------------------------------------------

Client Client::connect(std::string_view url, const ClientOptions& options) {
    auto impl = std::make_shared<detail::ClientImpl>();
    impl->options = options;
    impl->window = options.window;
    const bool local = url.rfind("unix://", 0) == 0 || url.rfind("ipc://", 0) == 0;
    if (local)
        impl->link = std::make_unique<detail::LocalClientLink>(std::string(url.substr(url.find("://") + 3)), options);
    else
        impl->link = std::make_unique<detail::WebSocketClientLink>(url, options, options.maxMessage);
    try {
        impl->send(detail::envelope([&](wire::Envelope::Builder& e) {
            e.setKind(wire::Kind::Hello);
            e.setProtocol(detail::kProtocol);
            e.setPeer(options.peer);
            if (!options.token.empty())
                e.setToken(options.token);
            e.setWindow(options.window);
            e.setMaxMessage(static_cast<uint32_t>(std::min<size_t>(options.maxMessage, UINT32_MAX)));
        }));
        const auto deadline = Clock::now() + options.connectTimeout;
        detail::Inbound inbound;
        if (!impl->link->receive(inbound, deadline))
            throw Error(Code::DeadlineExceeded, "no Welcome from the server");
        detail::Frame frame;
        if (!detail::parseFrame(inbound.buffer.data(), inbound.size, frame))
            throw Error(Code::DataLoss, "malformed frame from server");
        const wire::Envelope::Reader env = frame.envelope;
        if (env.kind() == wire::Kind::End)
            throw Error(static_cast<Code>(env.code()), std::string(env.message()), env.detail());
        if (env.kind() != wire::Kind::Welcome)
            throw Error(Code::DataLoss, "expected Welcome");
        impl->serverName = std::string(env.peer());
        impl->session = env.session();
        if (env.window())
            impl->window = static_cast<size_t>(env.window());
        impl->serverMaxMessage = env.maxMessage();
        impl->maxInflight = env.maxInflight();
        impl->slots.reserve(std::min<size_t>(impl->maxInflight ? impl->maxInflight : 64, 1024));
    } catch (const detail::LinkError& error) {
        impl->link->close();
        throw Error(Code::Unavailable, error.what());
    } catch (...) {
        impl->link->close();
        throw;
    }
    detail::ClientImpl* raw = impl.get();
    if (local)
        impl->leaderMode = true;
    else
        impl->reader = std::thread([raw] { raw->readerLoop(); });
    Client client;
    client.impl_ = std::move(impl);
    return client;
}

bool Client::connected() const noexcept {
    if (!impl_)
        return false;
    {
        std::lock_guard<std::mutex> lock(impl_->mu);
        if (impl_->closed)
            return false;
        if (!impl_->leaderMode || impl_->pumping)
            return true;
        impl_->pumping = true;
    }
    // A local connection has no reader thread to notice a vanished server: look now.
    impl_->pumpOnce(Clock::now());
    std::lock_guard<std::mutex> lock(impl_->mu);
    impl_->pumping = false;
    impl_->handoff();
    return !impl_->closed;
}

const std::string& Client::serverName() const {
    static const std::string none;
    return impl_ ? impl_->serverName : none;
}

uint64_t Client::sessionId() const { return impl_ ? impl_->session : 0; }

void Client::cancelAll() noexcept {
    if (!impl_)
        return;
    std::vector<uint32_t> ids;
    {
        std::lock_guard<std::mutex> lock(impl_->mu);
        for (auto& slot : impl_->slots)
            if (slot->id != 0 && !slot->ended)
                ids.push_back(slot->id);
    }
    for (uint32_t id : ids)
        impl_->cancelCall(id);
}

void Client::close() noexcept {
    if (impl_)
        impl_->close();
}

Payload Client::call(uint32_t method, const uint8_t* request, size_t size, const CallOptions& options) {
    if (!impl_)
        throw Error(Code::FailedPrecondition, "client is not connected");
    const auto deadline = options.timeout.count() > 0 ? Clock::now() + options.timeout : Clock::time_point::max();
    detail::CallState* state = impl_->start(method, request, size, options, false);
    const uint32_t id = state->id;
    std::unique_lock<std::mutex> lock(impl_->mu);
    const auto ready = [&] { return state->id != id || state->ended; };
    if (!impl_->wait(lock, state, ready, deadline)) {
        lock.unlock();
        impl_->cancelCall(id);
        throw Error(Code::DeadlineExceeded, "deadline exceeded");
    }
    if (state->id != id)
        throw Error(Code::Cancelled, "cancelled");
    Status status = std::move(state->status);
    Payload response = std::move(state->response);
    impl_->release(state);
    lock.unlock();
    if (!status.isOk())
        throw Error(std::move(status));
    if (!response)
        throw Error(Code::DataLoss, "the server sent no response");
    return response;
}

bool Client::notify(uint32_t method, const uint8_t* request, size_t size) noexcept {
    if (!impl_)
        return false;
    try {
        impl_->start(method, request, size, CallOptions{}, false, true);
        return true;
    } catch (...) {
        return false;
    }
}

RawStream Client::openStream(uint32_t method, const uint8_t* request, size_t size, const CallOptions& options) {
    if (!impl_)
        throw Error(Code::FailedPrecondition, "client is not connected");
    const auto deadline = options.timeout.count() > 0 ? Clock::now() + options.timeout : Clock::time_point::max();
    detail::CallState* state = impl_->start(method, request, size, options, true);
    return RawStream(impl_, state->id, deadline);
}

// --- RawStream ------------------------------------------------------------------------------------------------

RawStream::~RawStream() { cancel(); }

RawStream::RawStream(RawStream&& other) noexcept
    : impl_(std::move(other.impl_)), id_(std::exchange(other.id_, 0)), deadline_(other.deadline_) {}

RawStream& RawStream::operator=(RawStream&& other) noexcept {
    if (this != &other) {
        cancel();
        impl_ = std::move(other.impl_);
        id_ = std::exchange(other.id_, 0);
        deadline_ = other.deadline_;
    }
    return *this;
}

void RawStream::cancel() noexcept {
    if (impl_ && id_)
        impl_->cancelCall(id_);
    impl_.reset();
    id_ = 0;
}

bool RawStream::send(const uint8_t* message, size_t size) {
    if (!impl_ || !id_)
        return false;
    return impl_->sendItem(id_, message, size);
}

void RawStream::closeSend() {
    if (impl_ && id_)
        impl_->sendEnd(id_);
}

Payload RawStream::finish() {
    closeSend();
    Payload item;
    while (next(item)) {
    }
    return std::move(trailer_);  // a client-streaming call's response is the body of its End
}

bool RawStream::next(Payload& item) {
    if (!impl_ || !id_)
        return false;
    std::unique_lock<std::mutex> lock(impl_->mu);
    detail::CallState* state = impl_->find(id_);
    if (!state) {
        lock.unlock();
        impl_.reset();
        id_ = 0;
        throw Error(Code::Cancelled, "cancelled");
    }
    const uint32_t id = id_;
    const auto ready = [&] { return state->id != id || state->hasItems() || state->ended; };
    if (!impl_->wait(lock, state, ready, deadline_)) {
        lock.unlock();
        cancel();
        throw Error(Code::DeadlineExceeded, "deadline exceeded");
    }
    if (state->id == id && state->hasItems()) {
        detail::Item front = state->pop();
        const bool drained = !state->hasItems();
        lock.unlock();
        item = std::move(front.payload);
        impl_->consume(front.frameBytes, drained);
        return true;
    }
    Status status = state->id == id ? std::move(state->status) : Status(Code::Cancelled, "cancelled");
    if (state->id == id) {
        trailer_ = std::move(state->response);  // the closing message of the stream, if its method has one
        impl_->release(state);
    }
    lock.unlock();
    impl_.reset();
    id_ = 0;
    if (!status.isOk())
        throw Error(std::move(status));
    return false;
}

} // namespace rpc
