// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

#include "rpc/server.hpp"

#include <poll.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <condition_variable>
#include <cstdio>
#include <list>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <unordered_map>

#include "bufs/local.hpp"
#include "bufs/shm.hpp"
#include "arena.hpp"
#include "http/thread_pool.hpp"
#include "ipc_common.hpp"
#include "json/json.hpp"
#include "rpc/detail/frame.hpp"
#include "rpc/detail/link.hpp"

namespace rpc {

using Clock = std::chrono::steady_clock;

namespace detail {

// What the accept hook learned, handed to the session through WebSocketAccept::userData.
struct AcceptInfo {
    std::optional<Principal> principal;
};

// A block of the client's shared area lent to a call or a stream item. Holds the area, so it stays mapped (and
// registered) for as long as anything reads it.
struct BlobLease {
    ArenaRef arena;
    uint64_t offset = 0;
    uint64_t length = 0;
    uint32_t field = 0;
};

struct Leases {
    BlobLease items[kMaxBlobsPerMessage];
    uint8_t count = 0;

    void clear() noexcept {
        for (uint8_t i = 0; i < count; ++i)
            items[i] = BlobLease();
        count = 0;
    }
    const BlobLease* find(uint32_t field) const noexcept {
        for (uint8_t i = 0; i < count; ++i)
            if (items[i].field == field)
                return &items[i];
        return nullptr;
    }
};

inline Blob leaseBlob(const BlobLease& lease) {
    Blob blob;
    blob.data = lease.arena->base() + lease.offset;
    blob.size = static_cast<size_t>(lease.length);
    blob.device = lease.arena->device(lease.offset);
    blob.shared = true;
    return blob;
}

// An item of the client's stream that a handler took out of its reader (ServerReader::take). Pooled like calls.
struct TakenItem {
    EndpointImpl* owner = nullptr;
    std::shared_ptr<Session> session;
    Block frame;
    Leases leases;
};

// One call. Pooled: the objects are recycled through EndpointImpl, so a warm server allocates nothing per call,
// and the memory does not fragment. Reference counted by hand (CallRef) because std::shared_ptr would allocate.
struct ServerCall {
    std::atomic<uint32_t> refs{0};
    EndpointImpl* owner = nullptr;
    std::shared_ptr<Session> session;
    const Method* method = nullptr;
    uint32_t id = 0;
    Block frame;  // the whole Call frame; the request message is its body
    const uint8_t* request = nullptr;
    size_t requestSize = 0;
    Clock::time_point started;
    Clock::time_point deadline = Clock::time_point::max();
    std::atomic<bool> cancelled{false};
    std::atomic<bool> stalled{false};  // the client stopped reading its stream
    std::atomic<uint16_t> abortCode{0};  // a Code the call must end with (a stream item that broke the rules); 0 = none
    Leases blobs;  // the request's blobs; they return to the client with the call's End
    uint64_t responseBytes = 0;
    bufs::Buffer trailer;  // a stream's closing message, if the handler set one (it travels behind the Ok End)
    size_t trailerSize = 0;
    std::mutex hookMutex;
    std::function<void()> onCancel;  // set by the handler; run once when the call is cancelled

    // The client's stream (client streaming and bidirectional methods). Guarded by the session's mutex.
    struct InItem {
        Block frame;
        const uint8_t* data = nullptr;
        size_t size = 0;
        size_t frameBytes = 0;
        Leases leases;  // the item's blobs; they go back to the client in a Release frame when the item is dropped
    };
    std::vector<InItem> inbox;  // a ring that keeps its capacity
    size_t inHead = 0;
    size_t inCount = 0;
    bool inClosed = false;       // the client finished its stream
    uint64_t inBytes = 0;        // frame bytes in the inbox
    uint64_t inConsumed = 0;     // frame bytes read since the last Credit
    InItem current;              // the item the handler is looking at

    void pushItem(InItem&& item) {
        if (inCount == inbox.size()) {
            std::vector<InItem> grown(std::max<size_t>(8, inbox.size() * 2));
            for (size_t i = 0; i < inCount; ++i)
                grown[i] = std::move(inbox[(inHead + i) % inbox.size()]);
            inbox = std::move(grown);
            inHead = 0;
        }
        inbox[(inHead + inCount) % inbox.size()] = std::move(item);
        ++inCount;
    }
    InItem popItem() {
        InItem out = std::move(inbox[inHead]);
        inHead = (inHead + 1) % inbox.size();
        --inCount;
        return out;
    }
    void resetInbox() {
        while (inCount)
            popItem();
        inHead = 0;
        inClosed = false;
        inBytes = inConsumed = 0;
        current = InItem();
    }

    bool done() const;
    // Marks the call cancelled and runs the handler's hook (once). Any thread.
    void cancel();
};

class CallRef {
public:
    CallRef() = default;
    explicit CallRef(ServerCall* call) noexcept : call_(call) {
        if (call_)
            call_->refs.fetch_add(1, std::memory_order_relaxed);
    }
    CallRef(const CallRef& other) noexcept : CallRef(other.call_) {}
    CallRef(CallRef&& other) noexcept : call_(other.call_) { other.call_ = nullptr; }
    CallRef& operator=(CallRef other) noexcept {
        std::swap(call_, other.call_);
        return *this;
    }
    ~CallRef() { release(); }

    /// Takes over a reference that was added by hand (refs.fetch_add): how a call travels through a std::function,
    /// which stores only a trivially copyable capture without allocating.
    static CallRef adopt(ServerCall* call) noexcept {
        CallRef ref;
        ref.call_ = call;
        return ref;
    }

    ServerCall* get() const noexcept { return call_; }
    ServerCall* operator->() const noexcept { return call_; }
    ServerCall& operator*() const noexcept { return *call_; }
    explicit operator bool() const noexcept { return call_ != nullptr; }

private:
    void release() noexcept;
    ServerCall* call_ = nullptr;
};

struct LocalConnection;

struct EndpointImpl {
    explicit EndpointImpl(EndpointOptions o) : options(std::move(o)), arenaAccount(std::make_shared<ArenaAccount>()) {
        arenaAccount->cap = options.sharedArenaTotal_;
        arenaAccount->registration = options.sharedRegistration_;
    }

    EndpointOptions options;
    Registry registry;
    std::vector<std::shared_ptr<Service>> owned;
    http::Server* server = nullptr;
    size_t maxMessage = 0;  // the largest WebSocket message either way
    size_t window = 0;      // the window offered to clients

    std::unique_ptr<http::ThreadPool> pool;
    std::atomic<bool> closing{false};
    std::mutex shutdownMutex;
    bool shutdownDone = false;
    std::atomic<uint64_t> nextSession{1};

    std::mutex mutex;  // sessions, inflight
    std::condition_variable drained;
    std::unordered_map<uint64_t, std::shared_ptr<Session>> sessions;
    size_t inflight = 0;

    std::atomic<uint64_t> sessionsTotal{0};
    std::atomic<uint64_t> calls{0};
    std::atomic<uint64_t> failed{0};
    std::atomic<uint64_t> rejected{0};
    std::atomic<uint64_t> blobsShared{0};
    std::atomic<uint64_t> blobsInline{0};
    std::shared_ptr<ArenaAccount> arenaAccount;  // shared with the areas, which may outlive us (a held Taken)

    // Recycled stream items taken out of readers.
    std::mutex takenMutex;
    std::vector<std::unique_ptr<TakenItem>> allTaken;
    std::vector<TakenItem*> freeTaken;

    TakenItem* acquireTaken() {
        std::lock_guard<std::mutex> lock(takenMutex);
        if (freeTaken.empty()) {
            allTaken.push_back(std::make_unique<TakenItem>());
            allTaken.back()->owner = this;
            return allTaken.back().get();
        }
        TakenItem* item = freeTaken.back();
        freeTaken.pop_back();
        return item;
    }
    void recycleTaken(TakenItem* item) {
        std::lock_guard<std::mutex> lock(takenMutex);
        freeTaken.push_back(item);
    }

    // Recycled calls.
    std::mutex callMutex;
    std::vector<std::unique_ptr<ServerCall>> allCalls;
    std::vector<ServerCall*> freeCalls;

    // Local sockets.
    std::mutex localMutex;
    std::list<std::unique_ptr<LocalConnection>> localConnections;
    std::thread acceptor;
    bufs::local::Socket listener;
    std::string localPath;
    LocalOptions localOptions;
    size_t localMaxMessage = 0;
    uint64_t ringCapacity = 0;

    bool submit(std::function<void()> task) {
        if (options.executor_)
            return options.executor_(std::move(task));
        return pool->tryEnqueue(std::move(task));
    }

    void ensurePool() {
        if (!pool && !options.executor_)
            pool = std::make_unique<http::ThreadPool>(options.workers_ ? options.workers_ : 1, options.queueCapacity_, options.workerSpin_);
    }

    CallRef acquireCall() {
        ServerCall* call = nullptr;
        {
            std::lock_guard<std::mutex> lock(callMutex);
            if (!freeCalls.empty()) {
                call = freeCalls.back();
                freeCalls.pop_back();
            } else {
                allCalls.push_back(std::make_unique<ServerCall>());
                call = allCalls.back().get();
                call->owner = this;
            }
        }
        call->cancelled.store(false, std::memory_order_relaxed);
        call->stalled.store(false, std::memory_order_relaxed);
        call->abortCode.store(0, std::memory_order_relaxed);
        call->deadline = Clock::time_point::max();
        call->responseBytes = 0;
        call->trailerSize = 0;
        call->trailer.reset();
        {
            std::lock_guard<std::mutex> lock(call->hookMutex);
            call->onCancel = nullptr;
        }
        return CallRef(call);
    }

    void recycle(ServerCall* call) {
        call->session.reset();
        call->frame.reset();
        call->blobs.clear();
        call->resetInbox();
        call->request = nullptr;
        call->requestSize = 0;
        call->method = nullptr;
        std::lock_guard<std::mutex> lock(callMutex);
        freeCalls.push_back(call);
    }

    void attach(http::Server& target, std::string pattern);
    void listenLocal(const std::string& path, LocalOptions localOptions);
    void acceptLoop();
    void shutdown(std::chrono::milliseconds grace);
};

void CallRef::release() noexcept {
    if (call_ && call_->refs.fetch_sub(1, std::memory_order_acq_rel) == 1)
        call_->owner->recycle(call_);
    call_ = nullptr;
}

// ---- the protocol core ------------------------------------------------------------------------------------------

struct ReleaseEntry {
    uint64_t offset;
    uint64_t length;
};

class Session : public std::enable_shared_from_this<Session> {
public:
    friend class ::rpc::StreamReader;
    Session(EndpointImpl& endpoint, std::shared_ptr<ServerLink> link, std::optional<Principal> who, uint64_t id)
        : ep_(endpoint), link_(std::move(link)), id_(id), principal_(std::move(who)) {
        if (!principal_)
            principal_ = Principal{"anonymous", nullptr};
        calls_.reserve(ep_.options.maxInflight_);
        if (ep_.options.serialPerSession_)
            pending_.reserve(ep_.options.maxInflight_);
    }

    uint64_t id() const { return id_; }
    const InboundMemory& inboundMemory() const { return ep_.options.inboundMemory_; }
    const Principal& principal() const { return *principal_; }
    std::string_view remoteAddress() const { return link_->remoteAddress(); }
    bool closed() const { return closed_.load(std::memory_order_acquire); }
    void needsPrincipal(bool v) { needsPrincipal_ = v; }
    void setAuthenticated(bool v) { authenticatedByHandshake_ = v; }

    std::shared_ptr<void> state() const {
        std::lock_guard<std::mutex> lock(mu_);
        return state_;
    }
    void setState(std::shared_ptr<void> state) {
        std::lock_guard<std::mutex> lock(mu_);
        state_ = std::move(state);
    }

    // ---- frames in (the transport's reading thread) ----

    // `onReader`: the calling thread belongs to this connection alone (a local connection's reader), so a short
    // handler may run on it.
    void onFrame(Block frameBuffer, size_t size, bool onReader) {
        Frame frame;
        if (!parseFrame(frameBuffer.data(), size, frame))
            return fail(1002, "malformed frame");
        const wire::Envelope::Reader env = frame.envelope;
        if (phase_ == Phase::AwaitHello) {
            if (env.kind() != wire::Kind::Hello)
                return fail(1002, "expected Hello");
            return onHello(env);
        }
        switch (env.kind()) {
        case wire::Kind::Call: return onCall(env, std::move(frameBuffer), frame, onReader);
        case wire::Kind::Cancel: return onCancel(env.id());
        case wire::Kind::Data: return onClientData(env, std::move(frameBuffer), frame, size);
        case wire::Kind::End: return onClientEnd(env.id());
        case wire::Kind::Credit: return onCredit(env.credit());
        case wire::Kind::Ping: {
            const auto reply = envelope([&](wire::Envelope::Builder& e) {
                e.setId(env.id());
                e.setKind(wire::Kind::Pong);
            });
            link_->send(reply.first, reply.second, nullptr, 0);
            return;
        }
        case wire::Kind::Goodbye: return onGoodbye();
        default: return fail(1002, "unexpected frame");
        }
    }

    // The connection ended.
    void onClosed() {
        std::vector<CallRef> live;
        std::vector<CallRef> queued;
        std::shared_ptr<void> state;
        {
            std::lock_guard<std::mutex> lock(mu_);
            closed_.store(true, std::memory_order_release);
            live = calls_;
            queued.swap(pending_);
            state = std::move(state_);
        }
        for (CallRef& call : live)
            call->cancel();
        cv_.notify_all();
        // Calls that never started (serial sessions) end here.
        for (CallRef& call : queued)
            finishWithoutRunning(call);
        {
            std::lock_guard<std::mutex> lock(ep_.mutex);
            ep_.sessions.erase(id_);
        }
        if (state && ep_.options.onSessionClose_) {
            auto hook = ep_.options.onSessionClose_;
            const uint64_t id = id_;
            auto task = [hook, id, state] { hook(id, state); };
            if (!ep_.submit(task))
                task();
        }
    }

    // ---- any thread ----

    void closeConnection(uint16_t code, std::string reason) { link_->close(code, reason); }

    void sendClosing() {
        const auto env = envelope([](wire::Envelope::Builder& e) { e.setKind(wire::Kind::Closing); });
        link_->send(env.first, env.second, nullptr, 0);
    }

    void cancelAll() {
        std::vector<CallRef> live;
        {
            std::lock_guard<std::mutex> lock(mu_);
            live = calls_;
        }
        for (CallRef& call : live)
            call->cancel();
        cv_.notify_all();
    }

    void runCall(const CallRef& call) {
        const Method& method = *call->method;
        Status status;
        bool sentResponse = false;
        try {
            if (call->done()) {
                status = deadlineOrCancelled(*call);
            } else if (!call->requestSize || bufs::typeOf(call->request, call->requestSize) != method.requestType) {
                status = Status(Code::InvalidArgument, "request is not a " + method.name + " request");
            } else if (!method.verifyRequest(call->request, call->requestSize)) {
                status = Status(Code::InvalidArgument, "malformed request");
            } else {
                ServerContext context(*call);
                if (method.streaming == Streaming::None || method.streaming == Streaming::Client) {
                    bufs::MessageBuilder builder;
                    if (method.streaming == Streaming::None) {
                        status = method.unary(context, call->request, call->requestSize, builder);
                    } else {
                        StreamReader in(*call);
                        status = method.clientStream(context, call->request, call->requestSize, in, builder);
                        // A reader that returns false because the call was cancelled leaves the handler with partial input:
                        // what it computed from that is not an answer.
                        if (status.isOk() && call->done())
                            status = deadlineOrCancelled(*call);
                    }
                    if (status.isOk()) {
                        const auto sealed = builder.finish();
                        const auto env = envelope([&](wire::Envelope::Builder& e) {
                            e.setId(call->id);
                            e.setKind(wire::Kind::End);
                        });
                        if (env.second + sealed.second > maxFrame_) {
                            status = Status(Code::ResourceExhausted, "response exceeds the maximum message size");
                        } else {
                            // A response is the End frame with the message behind it: one send, one copy.
                            link_->send(env.first, env.second, sealed.first, sealed.second);
                            call->responseBytes = sealed.second;
                            sentResponse = true;
                        }
                    }
                } else if (method.streaming == Streaming::Bidi) {
                    StreamReader in(*call);
                    StreamWriter writer(*call);
                    status = method.bidi(context, call->request, call->requestSize, in, writer);
                    if (status.isOk() && call->done())
                        status = deadlineOrCancelled(*call);
                } else {
                    StreamWriter writer(*call);
                    status = method.stream(context, call->request, call->requestSize, writer);
                    if (status.isOk() && call->done()) {
                        status = deadlineOrCancelled(*call);
                    } else if (status.isOk() && call->trailerSize) {
                        // The stream's closing message travels behind its Ok End.
                        const auto env = envelope([&](wire::Envelope::Builder& e) {
                            e.setId(call->id);
                            e.setKind(wire::Kind::End);
                        });
                        if (env.second + call->trailerSize <= maxFrame_) {
                            link_->send(env.first, env.second, call->trailer.data(), call->trailerSize);
                            sentResponse = true;
                        } else {
                            status = Status(Code::ResourceExhausted, "trailer exceeds the maximum message size");
                        }
                    }
                }
            }
        } catch (const Error& error) {
            status = error.status();
        } catch (const std::exception& error) {
            status = Status(Code::Internal, ep_.options.exposeExceptions_ ? error.what() : "internal error");
        } catch (...) {
            status = Status(Code::Internal, "internal error");
        }
        if (method.streaming == Streaming::Client || method.streaming == Streaming::Bidi)
            releaseInbox(*call);
        if (const uint16_t aborted = call->abortCode.load(std::memory_order_acquire))
            status = Status(static_cast<Code>(aborted), "bad blob reference");
        else if (call->stalled.load())
            status = Status(Code::Unavailable, "client is not reading its stream");
        else if (status.code() == Code::Cancelled && ep_.closing.load())
            status = Status(Code::Unavailable, "server is shutting down");  // cancelled because we are going away
        if (!sentResponse)
            sendEnd(call->id, status);

        if (!status.isOk())
            ep_.failed.fetch_add(1, std::memory_order_relaxed);
        if (ep_.options.onCall_) {
            try {
                ep_.options.onCall_(CallInfo{principal(), method.name, status,
                                             static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
                                                                       Clock::now() - call->started)
                                                                       .count()),
                                             call->requestSize, call->responseBytes});
            } catch (...) {
            }
        }
        finish(call);
    }

    bool streamSend(ServerCall& call, const uint8_t* message, size_t size) {
        const auto env = envelope([&](wire::Envelope::Builder& e) {
            e.setId(call.id);
            e.setKind(wire::Kind::Data);
        });
        const size_t frameSize = env.second + size;
        if (frameSize > maxFrame_)
            throw Error(Code::ResourceExhausted, "stream item exceeds the maximum message size");
        {
            std::unique_lock<std::mutex> lock(mu_);
            const auto stallUntil = Clock::now() + ep_.options.stallTimeout_;
            const auto until = std::min(stallUntil, call.deadline);
            const auto ready = [&] {
                return closed() || call.cancelled.load() || unacked_ == 0 || unacked_ + frameSize <= window_;
            };
            if (!cv_.wait_until(lock, until, ready)) {
                lock.unlock();
                if (Clock::now() >= call.deadline)
                    return false;
                call.stalled.store(true);
                closeConnection(1008, "client is not reading");
                return false;
            }
            if (closed() || call.cancelled.load())
                return false;
            unacked_ += frameSize;
        }
        // `env` belongs to this thread's builder and nothing here has built another envelope since.
        if (!link_->send(env.first, env.second, message, size))
            return false;
        call.responseBytes += size;
        return true;
    }

    size_t maxFrame() const { return maxFrame_; }

    void setLimits(size_t maxFrame, size_t window) {
        maxFrame_ = maxFrame;
        window_ = window;
    }

private:
    enum class Phase { AwaitHello, Ready };

    void fail(uint16_t code, const char* reason) { link_->close(code, reason); }

    void onHello(const wire::Envelope::Reader& env) {
        if (env.protocol() != kProtocol) {
            rejectHello(Code::FailedPrecondition, "unsupported protocol version", 1002);
            return;
        }
        if (!authenticatedByHandshake_ && ep_.options.authenticate_ && !env.token().empty()) {
            if (auto who = ep_.options.authenticate_(env.token())) {
                principal_ = std::move(*who);
                authenticatedByHandshake_ = true;
            }
        }
        if (needsPrincipal_ && !authenticatedByHandshake_) {
            rejectHello(Code::Unauthenticated, "authentication required", 1008);
            return;
        }
        // The smaller of what both sides can take.
        size_t message = ep_.maxMessage;
        if (link_->maxFrame() && link_->maxFrame() < message)
            message = link_->maxFrame();
        if (env.maxMessage() && env.maxMessage() < message)
            message = env.maxMessage();
        size_t window = ep_.window;
        if (env.window() && env.window() < window)
            window = static_cast<size_t>(env.window());
        setLimits(message, window);
        phase_ = Phase::Ready;
        uint64_t arenaBytes = 0;
        if (env.arena() > 0)
            arenaBytes = adoptArena(env.arena());
        const auto welcome = envelope([&](wire::Envelope::Builder& e) {
            e.setKind(wire::Kind::Welcome);
            e.setProtocol(kProtocol);
            e.setPeer(ep_.options.serverName_);
            e.setSession(id_);
            e.setWindow(window);
            e.setMaxMessage(static_cast<uint32_t>(message));
            e.setMaxInflight(static_cast<uint32_t>(ep_.options.maxInflight_));
            if (arenaBytes)
                e.setArena(arenaBytes);
        });
        link_->send(welcome.first, welcome.second, nullptr, 0);
    }

    // The client announced a shared area of `bytes`: takes it from the link and validates it. Returns the bytes
    // accepted, 0 when the connection goes on without one (the client falls back to inline fields). The message is
    // always drained, so a refused memfd is closed here.
    uint64_t adoptArena(uint64_t bytes) {
        bufs::Fd fd;
        if (!link_->takeArena(bytes, fd))
            return 0;
        const uint64_t offeredMiB = std::min<uint64_t>((ep_.options.sharedArena_ + (1u << 20) - 1) >> 20, UINT32_MAX);
        if (offeredMiB == 0 || bytes > (offeredMiB << 20))
            return 0;
        const char* why = nullptr;
        SessionArena* arena = SessionArena::adopt(std::move(fd), bytes, ep_.arenaAccount, why);
        if (!arena)
            return 0;
        arena_ = ArenaRef::adopt(arena);
        return bytes;
    }

    void rejectHello(Code code, const char* message, uint16_t closeCode) {
        const auto env = envelope([&](wire::Envelope::Builder& e) {
            e.setKind(wire::Kind::End);
            e.setCode(static_cast<uint16_t>(code));
            e.setMessage(message);
        });
        link_->send(env.first, env.second, nullptr, 0);
        link_->close(closeCode, message);
    }

    void onCall(const wire::Envelope::Reader& env, Block frameBuffer, const Frame& frame, bool onReader) {
        const uint32_t id = env.id();
        if (id == 0)
            return fail(1002, "call id 0");
        if (!blobsAllowed(env))
            return;
        {
            std::lock_guard<std::mutex> lock(mu_);
            for (const CallRef& call : calls_)
                if (call->id == id)
                    return fail(1002, "duplicate call id");
        }
        ep_.calls.fetch_add(1, std::memory_order_relaxed);
        const auto reject = [&](Code code, std::string message) {
            ep_.rejected.fetch_add(1, std::memory_order_relaxed);
            ep_.failed.fetch_add(1, std::memory_order_relaxed);
            sendEnd(id, Status(code, std::move(message)));
        };
        if (ep_.closing.load())
            return reject(Code::Unavailable, "server is shutting down");
        if (goodbye_)
            return reject(Code::FailedPrecondition, "session is closing");
        const Method* method = ep_.registry.find(env.method());
        if (!method)
            return reject(Code::Unimplemented, "unknown method");
        Leases leases;
        if (env.hasBlobs() && !takeBlobs(env, method->sharedFields, leases))
            return reject(Code::InvalidArgument, "bad blob reference");
        CallRef call = ep_.acquireCall();
        call->blobs = std::move(leases);
        call->session = shared_from_this();
        call->method = method;
        call->id = id;
        call->started = Clock::now();
        if (env.deadlineMs())
            call->deadline = call->started + std::chrono::milliseconds(env.deadlineMs());
        // The request is read where it arrived: the frame's block stays with the call.
        call->request = frame.bodyData;
        call->requestSize = frame.bodySize;
        call->frame = std::move(frameBuffer);
        {
            std::lock_guard<std::mutex> lock(mu_);
            if (calls_.size() >= ep_.options.maxInflight_)
                return reject(Code::ResourceExhausted, "too many calls in flight on this session");
            calls_.push_back(call);
        }
        {
            std::lock_guard<std::mutex> lock(ep_.mutex);
            ++ep_.inflight;
        }
        schedule(call, onReader);
    }

    // Runs `call` now, queues it behind the running one (serial sessions), or hands it to a worker.
    void schedule(const CallRef& call, bool onReader) {
        if (ep_.options.serialPerSession_) {
            std::lock_guard<std::mutex> lock(mu_);
            if (running_) {
                pending_.push_back(call);
                return;
            }
            running_ = true;
        }
        launch(call, onReader);
    }

    void launch(const CallRef& call, bool onReader) {
        if (onReader && (ep_.options.inlineUnary_ || call->method->inlineHint) && link_->dedicatedReader() &&
            call->method->streaming == Streaming::None) {
            runCall(call);
            return;
        }
        ServerCall* raw = call.get();
        raw->refs.fetch_add(1, std::memory_order_relaxed);  // owned by the task
        if (!ep_.submit([raw] {
                CallRef owned = CallRef::adopt(raw);
                owned->session->runCall(owned);
            })) {
            CallRef::adopt(raw);  // the task will never run: give its reference back
            ep_.rejected.fetch_add(1, std::memory_order_relaxed);
            ep_.failed.fetch_add(1, std::memory_order_relaxed);
            sendEnd(call->id, Status(Code::Unavailable, "server is overloaded"));
            finish(call);
        }
    }

    void onCancel(uint32_t id) {
        CallRef target;
        {
            std::lock_guard<std::mutex> lock(mu_);
            for (const CallRef& call : calls_)
                if (call->id == id)
                    target = call;
        }
        if (target) {
            target->cancel();
            cv_.notify_all();
        }
    }

    // ---- the client's stream ----

    void sendCredit(uint64_t bytes) {
        const auto env = envelope([&](wire::Envelope::Builder& e) {
            e.setKind(wire::Kind::Credit);
            e.setCredit(bytes);
        });
        link_->send(env.first, env.second, nullptr, 0);
    }

    CallRef findCall(uint32_t id) {
        std::lock_guard<std::mutex> lock(mu_);
        for (const CallRef& call : calls_)
            if (call->id == id)
                return call;
        return CallRef();
    }

public:
    // ---- blobs ----

    // The protocol rules a frame with blobs must keep whatever the method: at most kMaxBlobsPerMessage, and only on a
    // connection that has a shared area. Breaking them closes the connection. False: it was closed.
    bool blobsAllowed(const wire::Envelope::Reader& env) {
        const size_t count = env.blobs().size();
        if (count == 0)
            return true;
        if (count > kMaxBlobsPerMessage) {
            fail(1002, "too many blobs");
            return false;
        }
        if (!arena_) {
            fail(1002, "blobs without a shared area");
            return false;
        }
        return true;
    }

    // Turns the blob references of a Call or Data frame into leases, if every one is valid: a field the method marks
    // shared (`sharedMask`, a bit per slot), 8-aligned, inside the area (no overflow), listed once. False: the call
    // (or item) is a client error; `out` is empty.
    bool takeBlobs(const wire::Envelope::Reader& env, uint64_t sharedMask, Leases& out) {
        out.count = 0;
        const bufs::Vector<wire::BlobRef> refs = env.blobs();
        const uint64_t bytes = arena_->bytes();
        for (uint32_t i = 0; i < refs.size(); ++i) {
            const wire::BlobRef& ref = refs[i];
            bool ok = ref.field < 64 && ((sharedMask >> ref.field) & 1) && ref.offset % 8 == 0 && ref.offset <= bytes &&
                      ref.length <= bytes - ref.offset;
            for (uint8_t j = 0; ok && j < out.count; ++j)
                ok = out.items[j].field != ref.field;
            if (!ok) {
                out.clear();
                return false;
            }
            BlobLease& lease = out.items[out.count++];
            lease.arena = ArenaRef(arena_.get());
            lease.offset = ref.offset;
            lease.length = ref.length;
            lease.field = ref.field;
        }
        ep_.blobsShared.fetch_add(refs.size(), std::memory_order_relaxed);
        return true;
    }

    // ---- giving blocks back ----

    // Appends to the batch of blocks the client may reuse; a full batch goes out at once. Any thread; never call it
    // with a lock held (it may send).
    void queueRelease(uint64_t offset, uint64_t length) {
        ReleaseEntry batch[kReleaseBatch];
        size_t n = 0;
        {
            std::lock_guard<std::mutex> lock(releaseMu_);
            releaseQueue_[releaseCount_++] = {offset, length};
            releasePending_.store(static_cast<uint32_t>(releaseCount_), std::memory_order_relaxed);
            if (releaseCount_ == kReleaseBatch) {
                std::copy(releaseQueue_, releaseQueue_ + releaseCount_, batch);
                n = releaseCount_;
                releaseCount_ = 0;
                releasePending_.store(0, std::memory_order_relaxed);
            }
        }
        if (n)
            sendRelease(batch, n);
    }

    // Sends what is queued as one Release frame.
    void flushReleases() {
        if (releasePending_.load(std::memory_order_relaxed) == 0)
            return;
        ReleaseEntry batch[kReleaseBatch];
        size_t n = 0;
        {
            std::lock_guard<std::mutex> lock(releaseMu_);
            std::copy(releaseQueue_, releaseQueue_ + releaseCount_, batch);
            n = releaseCount_;
            releaseCount_ = 0;
            releasePending_.store(0, std::memory_order_relaxed);
        }
        if (n)
            sendRelease(batch, n);
    }

    // The reading thread has nothing to read: a good moment to tell the client what it can reuse.
    void onIdle() { flushReleases(); }

    // An item's blobs are not needed any more.
    void dropLeases(Leases& leases, bool flushNow) {
        for (uint8_t i = 0; i < leases.count; ++i)
            queueRelease(leases.items[i].offset, leases.items[i].length);
        leases.clear();
        if (flushNow)
            flushReleases();
    }

    // Queues every reference the frame lists, valid or not: the client counts them as lent until told otherwise.
    void releaseListed(const wire::Envelope::Reader& env) {
        for (const wire::BlobRef& ref : env.blobs())
            queueRelease(ref.offset, ref.length);
    }

    void sendRelease(const ReleaseEntry* entries, size_t count) {
        const auto env = envelope([&](wire::Envelope::Builder& e) {
            e.setKind(wire::Kind::Release);
            auto list = e.initBlobs(count);
            for (size_t i = 0; i < count; ++i) {
                wire::BlobRef ref;
                ref.field = 0;
                ref.reserved = 0;
                ref.offset = entries[i].offset;
                ref.length = entries[i].length;
                list.set(i, ref);
            }
        });
        link_->send(env.first, env.second, nullptr, 0);
    }

    // The handler's current item is finished with: its blobs go back to the client (batched).
    void dropCurrent(ServerCall& call) {
        dropLeases(call.current.leases, false);
        call.current = ServerCall::InItem();
    }

    // ServerReader::take: the current item, with its frame and blobs, handed to the handler for good.
    TakenItem* takeCurrent(ServerCall& call) {
        if (!call.current.frame)
            return nullptr;
        TakenItem* item = ep_.acquireTaken();
        item->session = shared_from_this();
        item->frame = std::move(call.current.frame);
        item->leases = std::move(call.current.leases);
        call.current = ServerCall::InItem();
        return item;
    }

    // An item of the client's stream. The window the client may fill is the one the server granted; an item for a call
    // that is not reading any more is dropped and its bytes credited at once, so the client's window never leaks.
    void onClientData(const wire::Envelope::Reader& env, Block frameBuffer, const Frame& frame, size_t frameBytes) {
        if (!blobsAllowed(env))
            return;
        CallRef call = findCall(env.id());
        const bool reading = call && (call->method->streaming == Streaming::Client || call->method->streaming == Streaming::Bidi);
        Leases leases;
        bool badBlobs = false;
        if (reading && env.hasBlobs() && !takeBlobs(env, call->method->itemSharedFields, leases))
            badBlobs = true;
        if (!reading || !frame.bodySize || badBlobs) {
            if (env.hasBlobs())
                releaseListed(env);  // nobody will read them
            if (badBlobs) {
                // A client bug, not a dead connection: this call ends InvalidArgument.
                call->abortCode.store(static_cast<uint16_t>(Code::InvalidArgument), std::memory_order_release);
                call->cancel();
                cv_.notify_all();
            }
            sendCredit(frameBytes);
            return;
        }
        bool dropped = false;
        {
            std::lock_guard<std::mutex> lock(mu_);
            if (call->inClosed) {
                dropped = true;
            } else if (call->inBytes + frameBytes > 2 * window_ + maxFrame_) {
                lockViolation_ = true;
            } else {
                ServerCall::InItem item;
                item.frame = std::move(frameBuffer);
                item.data = frame.bodyData;
                item.size = frame.bodySize;
                item.frameBytes = frameBytes;
                item.leases = std::move(leases);
                call->inBytes += frameBytes;
                call->pushItem(std::move(item));
            }
        }
        if (dropped) {
            if (env.hasBlobs())
                releaseListed(env);
            sendCredit(frameBytes);
            return;
        }
        if (lockViolation_)
            return fail(1008, "client exceeded its window");
        cv_.notify_all();
    }

private:
    void onClientEnd(uint32_t id) {
        CallRef call = findCall(id);
        if (!call)
            return;
        {
            std::lock_guard<std::mutex> lock(mu_);
            call->inClosed = true;
        }
        cv_.notify_all();
    }

    // StreamReader::next. Hands the handler the next item and credits what has been read once a quarter window has piled
    // up or the queue is empty (a sender holding a frame larger than the room left would otherwise wait forever).
    // The previous item is done with: its blobs are queued for the client, and flushed whenever the handler is about to
    // wait or the client is about to be credited.
    bool readerNext(ServerCall& call, const uint8_t*& message, size_t& size) {
        dropCurrent(call);
        uint64_t report = 0;
        {
            std::unique_lock<std::mutex> lock(mu_);
            if (call.inCount == 0 && !call.inClosed && !call.cancelled.load() && !closed()) {
                lock.unlock();
                flushReleases();  // about to wait: tell the client what it can reuse
                lock.lock();
            }
            cv_.wait(lock, [&] { return call.inCount != 0 || call.inClosed || call.cancelled.load() || closed(); });
            if (call.cancelled.load() || closed() || call.inCount == 0)
                return false;
            ServerCall::InItem item = call.popItem();
            call.inBytes -= item.frameBytes;
            call.inConsumed += item.frameBytes;
            call.current = std::move(item);
            if (call.inCount == 0 || call.inConsumed >= std::max<size_t>(window_ / 4, 1)) {
                report = call.inConsumed;
                call.inConsumed = 0;
            }
        }
        if (report) {
            flushReleases();
            sendCredit(report);
        }
        message = call.current.data;
        size = call.current.size;
        return true;
    }

    // The handler is done: whatever the client sent that nobody read is credited, so its window is whole again, and the
    // blobs of every item go back.
    void releaseInbox(ServerCall& call) {
        uint64_t leftover = 0;
        dropCurrent(call);
        for (;;) {
            ServerCall::InItem item;
            {
                std::lock_guard<std::mutex> lock(mu_);
                if (call.inCount == 0) {
                    leftover = call.inBytes + call.inConsumed;
                    call.inBytes = call.inConsumed = 0;
                    call.inClosed = true;
                    break;
                }
                item = call.popItem();
            }
            dropLeases(item.leases, false);
        }
        flushReleases();
        if (leftover && !closed())
            sendCredit(leftover);
    }

    void onCredit(uint64_t bytes) {
        {
            std::lock_guard<std::mutex> lock(mu_);
            unacked_ -= std::min<uint64_t>(unacked_, bytes);
        }
        cv_.notify_all();
    }

    void onGoodbye() {
        bool idle = false;
        {
            std::lock_guard<std::mutex> lock(mu_);
            goodbye_ = true;
            idle = calls_.empty();
        }
        if (idle)
            link_->close(1000, "goodbye");
    }

    // ---- helpers ----

    static Status deadlineOrCancelled(const ServerCall& call) {
        if (Clock::now() >= call.deadline)
            return Status(Code::DeadlineExceeded, "deadline exceeded");
        return Status(Code::Cancelled, "cancelled");
    }

    void sendEnd(uint32_t id, const Status& status) {
        const auto env = envelope([&](wire::Envelope::Builder& e) {
            e.setId(id);
            e.setKind(wire::Kind::End);
            e.setCode(static_cast<uint16_t>(status.code()));
            if (!status.message().empty())
                e.setMessage(clipMessage(status.message()));
            if (status.detail())
                e.setDetail(status.detail());
        });
        link_->send(env.first, env.second, nullptr, 0);
    }

    // A queued call whose session closed before it began.
    void finishWithoutRunning(const CallRef& call) {
        {
            std::lock_guard<std::mutex> lock(mu_);
            eraseCall(call.get());
        }
        decrementInflight();
    }

    void eraseCall(const ServerCall* call) {
        for (size_t i = 0; i < calls_.size(); ++i) {
            if (calls_[i].get() == call) {
                calls_[i] = std::move(calls_.back());
                calls_.pop_back();
                return;
            }
        }
    }

    void finish(const CallRef& call) {
        CallRef next;
        bool closeNow = false;
        {
            std::lock_guard<std::mutex> lock(mu_);
            eraseCall(call.get());
            closeNow = goodbye_ && calls_.empty();
            if (ep_.options.serialPerSession_) {
                if (!pending_.empty() && !closed()) {
                    next = pending_.front();
                    pending_.erase(pending_.begin());
                } else {
                    running_ = false;
                }
            }
        }
        decrementInflight();
        if (closeNow)
            link_->close(1000, "goodbye");
        if (next)
            launch(next, false);
    }

    void decrementInflight() {
        std::lock_guard<std::mutex> lock(ep_.mutex);
        if (--ep_.inflight == 0)
            ep_.drained.notify_all();
    }

    EndpointImpl& ep_;
    std::shared_ptr<ServerLink> link_;
    const uint64_t id_;
    std::optional<Principal> principal_;
    bool authenticatedByHandshake_ = false;
    bool needsPrincipal_ = false;
    Phase phase_ = Phase::AwaitHello;  // the reading thread only
    size_t maxFrame_ = 1 << 20;
    size_t window_ = 1 << 20;

    std::atomic<bool> closed_{false};
    mutable std::mutex mu_;  // calls_, pending_, running_, unacked_, goodbye_, state_
    std::condition_variable cv_;
    std::vector<CallRef> calls_;    // at most maxInflight; searched linearly
    std::vector<CallRef> pending_;  // serial sessions: calls waiting for the running one
    bool running_ = false;
    uint64_t unacked_ = 0;
    bool goodbye_ = false;
    bool lockViolation_ = false;
    std::shared_ptr<void> state_;

    ArenaRef arena_;  // the client's shared area, if one was accepted (set at Hello; the reading thread)

    // Blocks the client may reuse, batched into Release frames.
    static constexpr size_t kReleaseBatch = 64;
    std::mutex releaseMu_;
    ReleaseEntry releaseQueue_[kReleaseBatch];
    size_t releaseCount_ = 0;
    std::atomic<uint32_t> releasePending_{0};
};

void ServerCall::cancel() {
    if (cancelled.exchange(true, std::memory_order_acq_rel))
        return;
    // Under the lock, so that ServerContext::onCancel({}) -- which the handler calls before its state goes away --
    // waits for a hook that is running: the hook never outlives what it refers to. It must be short and must not wait.
    std::lock_guard<std::mutex> lock(hookMutex);
    if (onCancel) {
        try {
            onCancel();
        } catch (...) {
        }
    }
}

bool ServerCall::done() const {
    return cancelled.load(std::memory_order_acquire) || session->closed() || Clock::now() >= deadline;
}

// ---- WebSocket transport ------------------------------------------------------------------------------------

// Frames are queued by any thread and written by the connection's loop thread, in order. At most one post() to the
// loop is outstanding per burst.
class WebSocketServerLink final : public ServerLink, public std::enable_shared_from_this<WebSocketServerLink> {
public:
    WebSocketServerLink(EndpointImpl& endpoint, std::shared_ptr<http::WebSocketConnection> socket)
        : ep_(endpoint), ws_(std::move(socket)) {}

    bool send(const uint8_t* envelope, size_t envelopeSize, const uint8_t* body, size_t bodySize) override {
        // A block of the server's buffer pool: libhttp writes frames of 8 KiB and more from the block itself.
        http::Buffer frame(ep_.server->bufferPool(), envelopeSize + bodySize);
        frame.append(envelope, envelopeSize);
        if (bodySize)
            frame.append(body, bodySize);
        {
            std::lock_guard<std::mutex> lock(outMu_);
            if (closed_)
                return false;
            outbox_.push_back(std::move(frame));
            if (flushPending_)
                return true;
            flushPending_ = true;
            // The posted task holds only `this` (so std::function keeps it inline); this reference keeps the link alive
            // until every posted task has run.
            ++postedTasks_;
            if (!keepAlive_)
                keepAlive_ = shared_from_this();
        }
        postFlush();
        return true;
    }

    void close(uint16_t code, std::string_view reason) override {
        auto self = shared_from_this();
        std::string text(reason);
        for (int attempt = 0; attempt < 1000 && !isClosed(); ++attempt) {
            if (ws_->post([self, code, text] {
                    self->flush();
                    self->ws_->close(code, text);
                }))
                return;
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }

    std::string_view remoteAddress() const override { return ws_->remoteAddress(); }

    void markClosed() {
        std::lock_guard<std::mutex> lock(outMu_);
        closed_ = true;
        outbox_.clear();
    }

private:
    bool isClosed() {
        std::lock_guard<std::mutex> lock(outMu_);
        return closed_;
    }

    void postFlush() {
        // [this] is trivially copyable, so std::function keeps it in place instead of allocating.
        for (int attempt = 0; !isClosed(); ++attempt) {
            if (ws_->post([this] { flushTask(); }))
                return;
            if (attempt > 5000)  // the loop's task queue stayed full for seconds: give up on this client
                break;
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        std::shared_ptr<WebSocketServerLink> keep;
        {
            std::lock_guard<std::mutex> lock(outMu_);
            flushPending_ = false;
            if (--postedTasks_ == 0)
                keep = std::move(keepAlive_);
        }
    }

    // The posted task. Loop thread.
    void flushTask() {
        flush();
        std::shared_ptr<WebSocketServerLink> keep;
        {
            std::lock_guard<std::mutex> lock(outMu_);
            if (--postedTasks_ == 0)
                keep = std::move(keepAlive_);
        }
        // `keep` (possibly the last reference) is released here, after the last use of members.
    }

    // Loop thread.
    void flush() {
        {
            std::lock_guard<std::mutex> lock(outMu_);
            batch_.swap(outbox_);
            flushPending_ = false;
        }
        for (http::Buffer& frame : batch_)
            ws_->send(std::move(frame), true);
        batch_.clear();
    }

    EndpointImpl& ep_;
    std::shared_ptr<http::WebSocketConnection> ws_;
    std::mutex outMu_;
    std::vector<http::Buffer> outbox_;
    std::vector<http::Buffer> batch_;  // loop thread only
    bool flushPending_ = false;
    bool closed_ = false;
    std::shared_ptr<WebSocketServerLink> keepAlive_;  // held while postedTasks_ > 0
    int postedTasks_ = 0;                             // flush tasks posted to the loop and not yet run
};

// ---- local transport: a socket for the handshake and for noticing a dead peer, two shared-memory rings for the frames

class LocalServerLink final : public ServerLink {
public:
    LocalServerLink(EndpointImpl& endpoint, bufs::local::Socket socket, bufs::shm::Channel channel, std::string peer)
        : ep_(endpoint), socket_(std::move(socket)), channel_(std::move(channel)), peer_(std::move(peer)),
          spin_(std::chrono::duration_cast<std::chrono::nanoseconds>(endpoint.options.ipcSpin_)) {}

    bool send(const uint8_t* envelope, size_t envelopeSize, const uint8_t* body, size_t bodySize) override {
        const size_t total = envelopeSize + bodySize;
        std::lock_guard<std::mutex> lock(sendMutex_);
        if (closed_.load())
            return false;
        try {
            // Waits for the client to free room; the stream window normally keeps the ring from filling.
            uint8_t* slot = channel_.send().reserve(total, ep_.options.stallTimeout_, spin_);
            if (!slot) {
                abort();  // the client stopped reading
                return false;
            }
            std::memcpy(slot, envelope, envelopeSize);
            if (bodySize)
                std::memcpy(slot + envelopeSize, body, bodySize);
            channel_.send().commit(total);
            return true;
        } catch (const std::exception&) {
            abort();
            return false;
        }
    }

    // Frames already in the ring stay there for the peer to read; then it sees the connection end.
    void close(uint16_t, std::string_view) override {
        closed_.store(true);
        socket_.shutdown();
    }

    std::string_view remoteAddress() const override { return peer_; }
    bool dedicatedReader() const noexcept override { return true; }
    bool takeArena(uint64_t bytes, bufs::Fd& fd) override {
        // The client sent it before its Hello; the socket is non-blocking, so a message that never came is just absent.
        IpcArena message;
        bufs::local::ReceivedFds fds;
        long got = 0;
        try {
            got = socket_.receive(&message, sizeof message, fds);
        } catch (const std::exception&) {
            return false;
        }
        if (got != static_cast<long>(sizeof message) || fds.count != 1 || message.magic != IpcArena::kMagic ||
            message.version != 1 || message.bytes != bytes)
            return false;  // fds closes whatever came
        fd = fds.take(0);
        return true;
    }
    size_t maxFrame() const noexcept override { return const_cast<bufs::shm::Channel&>(channel_).send().maxMessageBytes(); }

    // The reading thread.
    void run(const std::shared_ptr<Session>& session) {
        bufs::shm::Ring& ring = channel_.receive();
        try {
            while (!closed_.load()) {
                const uint8_t* data = nullptr;
                size_t size = 0;
                if (ring.front(data, size)) {
                    // Copied out of the shared memory: the peer is another process and may change it under us.
                    Block frame = copyInbound(ep_.options.inboundMemory_, data, size);
                    ring.pop();
                    session->onFrame(std::move(frame), size, true);
                    continue;
                }
                session->onIdle();
                if (peerGone())
                    break;
                ring.wait(std::chrono::milliseconds(50), spin_);  // slices: close() and a dead peer are noticed
            }
        } catch (const std::exception&) {
            // a corrupt ring: the peer is gone as far as we are concerned
        }
        closed_.store(true);
        socket_.shutdown();
        session->onClosed();
    }

private:
    void abort() {
        closed_.store(true);
        socket_.shutdown();
    }

    bool peerGone() {
        pollfd p{socket_.fd(), POLLRDHUP, 0};
        return ::poll(&p, 1, 0) > 0 && (p.revents & (POLLHUP | POLLERR | POLLRDHUP));
    }

    EndpointImpl& ep_;
    bufs::local::Socket socket_;
    bufs::shm::Channel channel_;
    std::string peer_;
    std::chrono::nanoseconds spin_;
    std::mutex sendMutex_;
    std::atomic<bool> closed_{false};
};

struct LocalConnection {
    std::shared_ptr<LocalServerLink> link;
    std::thread thread;
    std::atomic<bool> done{false};
};

// ---- endpoint -----------------------------------------------------------------------------------------------------

void EndpointImpl::attach(http::Server& target, std::string pattern) {
    server = &target;
    const http::ServerConfig& config = target.config();
    maxMessage = options.maxMessage_ ? options.maxMessage_ : config.maxWebSocketMessageBytes;
    // Stream items in flight stay below what the connection may queue, or libhttp closes the connection.
    window = std::min(options.window_, config.maxWebSocketQueuedBytes / 2);
    if (window == 0)
        window = 1;
    ensurePool();

    http::WebSocketRouteOptions route;
    route.subprotocols = {kSubprotocol};
    route.maxMessageBytes = maxMessage;
    route.idleTimeoutSeconds = static_cast<int>(options.idleTimeout_.count());
    route.accept = [this](const http::WebSocketHandshakeRequest& request) {
        http::WebSocketAccept decision;
        if (closing.load()) {
            decision.status = 503;
            return decision;
        }
        {
            std::lock_guard<std::mutex> lock(mutex);
            if (sessions.size() >= options.maxSessions_) {
                decision.status = 503;
                return decision;
            }
        }
        auto info = std::make_shared<AcceptInfo>();
        if (options.authorize_) {
            Handshake handshake;
            handshake.path = request.request.path;
            handshake.remoteAddress = request.remoteAddress;
            handshake.header = [&request](std::string_view name) { return request.request.headers.get(name); };
            handshake.routeParam = [&request](std::string_view name) -> std::string_view {
                for (const auto& param : request.routeParams)
                    if (param.first == name)
                        return param.second;
                return {};
            };
            EndpointOptions::AuthorizeResult result = options.authorize_(handshake);
            if (const int* status = std::get_if<int>(&result)) {
                if (*status != 0) {
                    decision.status = *status;
                    return decision;
                }
            } else {
                info->principal = std::move(std::get<Principal>(result));
            }
        }
        decision.status = 101;
        decision.subprotocol = kSubprotocol;
        decision.userData = info;
        return decision;
    };

    target.onWebSocket(
        std::move(pattern),
        [this](std::shared_ptr<http::WebSocketConnection> socket) {
            auto info = std::static_pointer_cast<AcceptInfo>(socket->userData());
            const bool known = info && info->principal;
            const uint64_t id = nextSession.fetch_add(1);
            auto link = std::make_shared<WebSocketServerLink>(*this, socket);
            auto session = std::make_shared<Session>(*this, link, known ? std::move(info->principal) : std::nullopt, id);
            session->setAuthenticated(known);
            session->needsPrincipal(options.requireAuth_);
            session->setLimits(maxMessage, window);
            {
                std::lock_guard<std::mutex> lock(mutex);
                sessions.emplace(id, session);
            }
            sessionsTotal.fetch_add(1, std::memory_order_relaxed);
            socket->onMessage([session, link, memory = options.inboundMemory_](std::string_view data, bool binary) {
                if (!binary) {
                    link->close(1003, "text frames are not supported");
                    return;
                }
                // One copy, into the aligned block (pooled, or the caller's InboundMemory) the call keeps if this is a Call.
                session->onFrame(copyInbound(memory, reinterpret_cast<const uint8_t*>(data.data()), data.size()), data.size(), false);
            });
            socket->onClose([session, link] {
                link->markClosed();
                session->onClosed();
            });
        },
        std::move(route));
}

void EndpointImpl::listenLocal(const std::string& path, LocalOptions local) {
    if (acceptor.joinable())
        throw std::logic_error("rpc: listenLocal() was already called");
    localOptions = local;
    localPath = path;
    localMaxMessage = options.maxMessage_ ? options.maxMessage_ : (maxMessage ? maxMessage : (1 << 20));
    if (!maxMessage)
        maxMessage = localMaxMessage;
    if (!window)
        window = std::max<size_t>(options.window_, 1);
    ringCapacity = ringCapacityFor(localMaxMessage);
    ensurePool();
    listener = bufs::local::Socket::listen(path, static_cast<mode_t>(local.mode));
    acceptor = std::thread([this] { acceptLoop(); });
}

void EndpointImpl::acceptLoop() {
    while (!closing.load()) {
        pollfd p{listener.fd(), POLLIN, 0};
        if (::poll(&p, 1, 100) > 0 && (p.revents & POLLIN)) {
            for (;;) {
                bufs::local::Socket socket;
                try {
                    socket = listener.accept();
                } catch (const std::exception&) {
                    break;
                }
                if (!socket)
                    break;
                try {
                    const ucred peer = socket.peer();
                    IpcHello hello;
                    hello.ringBytes = ringCapacity;
                    hello.arenaMiB = static_cast<uint32_t>(std::min<uint64_t>((options.sharedArena_ + (1u << 20) - 1) >> 20, UINT32_MAX));
                    std::optional<Principal> principal;
                    unsigned refusal = 0;
                    {
                        std::lock_guard<std::mutex> lock(mutex);
                        if (closing.load() || sessions.size() >= options.maxSessions_)
                            refusal = 503;
                    }
                    const std::string peerText = "pid:" + std::to_string(peer.pid) + " uid:" + std::to_string(peer.uid);
                    if (!refusal && options.authorize_) {
                        Handshake handshake;
                        handshake.path = localPath;
                        handshake.remoteAddress = peerText;
                        handshake.local = true;
                        handshake.pid = peer.pid;
                        handshake.uid = static_cast<int>(peer.uid);
                        handshake.gid = static_cast<int>(peer.gid);
                        handshake.header = [](std::string_view) -> std::string_view { return {}; };
                        handshake.routeParam = handshake.header;
                        EndpointOptions::AuthorizeResult result = options.authorize_(handshake);
                        if (const int* status = std::get_if<int>(&result)) {
                            if (*status != 0)
                                refusal = static_cast<unsigned>(*status);
                        } else {
                            principal = std::move(std::get<Principal>(result));
                        }
                    }
                    if (refusal) {
                        hello.status = refusal;
                        socket.send(&hello, sizeof hello);
                        continue;
                    }
                    bufs::shm::Channel channel = bufs::shm::Channel::createAnonymous("librpc", ringCapacity);
                    const int fd = channel.fd();
                    if (!socket.send(&hello, sizeof hello, &fd, 1))
                        continue;
                    const uint64_t id = nextSession.fetch_add(1);
                    const bool known = principal.has_value();
                    auto link = std::make_shared<LocalServerLink>(*this, std::move(socket), std::move(channel), peerText);
                    auto session = std::make_shared<Session>(*this, link, std::move(principal), id);
                    session->setAuthenticated(known);
                    session->needsPrincipal(options.requireAuth_);
                    session->setLimits(std::min(maxMessage, localMaxMessage), window);
                    {
                        std::lock_guard<std::mutex> lock(mutex);
                        sessions.emplace(id, session);
                    }
                    sessionsTotal.fetch_add(1, std::memory_order_relaxed);
                    auto connection = std::make_unique<LocalConnection>();
                    connection->link = link;
                    LocalConnection* raw = connection.get();
                    raw->thread = std::thread([link, session, raw] {
                        link->run(session);
                        raw->done.store(true);
                    });
                    std::lock_guard<std::mutex> lock(localMutex);
                    localConnections.push_back(std::move(connection));
                } catch (const std::exception&) {
                    // a client that vanished during the handshake
                }
            }
        }
        // Reap connections whose reader thread has finished.
        std::lock_guard<std::mutex> lock(localMutex);
        for (auto it = localConnections.begin(); it != localConnections.end();) {
            if ((*it)->done.load()) {
                (*it)->thread.join();
                it = localConnections.erase(it);
            } else {
                ++it;
            }
        }
    }
}

void EndpointImpl::shutdown(std::chrono::milliseconds grace) {
    // A second caller (the destructor, say) waits for the first to finish rather than racing it.
    std::lock_guard<std::mutex> once(shutdownMutex);
    if (shutdownDone)
        return;
    closing.store(true);
    if (acceptor.joinable()) {
        listener.shutdown();  // wakes the accept thread's poll at once
        acceptor.join();
    }
    if (!localPath.empty())
        ::unlink(localPath.c_str());
    std::vector<std::shared_ptr<Session>> live;
    {
        std::lock_guard<std::mutex> lock(mutex);
        for (auto& entry : sessions)
            live.push_back(entry.second);
    }
    for (auto& session : live)
        session->sendClosing();
    {
        std::unique_lock<std::mutex> lock(mutex);
        drained.wait_for(lock, grace, [this] { return inflight == 0; });
    }
    for (auto& session : live)
        session->cancelAll();
    {
        std::unique_lock<std::mutex> lock(mutex);
        drained.wait_for(lock, std::chrono::seconds(2), [this] { return inflight == 0; });
    }
    for (auto& session : live)
        session->closeConnection(1001, "server is shutting down");
    // Local readers end once their link is closed.
    std::list<std::unique_ptr<LocalConnection>> connections;
    {
        std::lock_guard<std::mutex> lock(localMutex);
        connections.swap(localConnections);
    }
    for (auto& connection : connections) {
        connection->link->close(1001, "");
        connection->thread.join();
    }
    if (pool)
        pool->shutdown();
    shutdownDone = true;
}

} // namespace detail

// --- ServerContext, StreamWriter ---------------------------------------------------------------------------

uint64_t ServerContext::sessionId() const { return call_->session->id(); }
uint32_t ServerContext::callId() const { return call_->id; }
std::string_view ServerContext::method() const { return call_->method->name; }
const Principal& ServerContext::principal() const { return call_->session->principal(); }
std::string_view ServerContext::remoteAddress() const { return call_->session->remoteAddress(); }
bool ServerContext::cancelled() const { return call_->done(); }
Clock::time_point ServerContext::deadline() const { return call_->deadline; }
size_t ServerContext::maxMessage() const { return call_->session->maxFrame(); }
void ServerContext::onCancel(std::function<void()> hook) const {
    std::lock_guard<std::mutex> lock(call_->hookMutex);
    call_->onCancel = std::move(hook);
    if (call_->onCancel && call_->cancelled.load(std::memory_order_acquire)) {  // cancelled before the hook was set
        try {
            call_->onCancel();
        } catch (...) {
        }
    }
}
std::shared_ptr<void> ServerContext::sessionState() const { return call_->session->state(); }
void ServerContext::setSessionState(std::shared_ptr<void> state) const { call_->session->setState(std::move(state)); }

void StreamWriter::setTrailer(const uint8_t* message, size_t size) {
    call_->trailer = detail::alignedCopy(message, size);
    call_->trailerSize = size;
}

bool StreamReader::next(const uint8_t*& message, size_t& size) { return call_->session->readerNext(*call_, message, size); }

namespace {
Blob inlineBlobOf(detail::EndpointImpl& ep, const InboundMemory& memory, const uint8_t* data, size_t size) {
    Blob blob;
    blob.data = data;
    blob.size = size;
    if (size) {
        ep.blobsInline.fetch_add(1, std::memory_order_relaxed);
        if (memory.device)
            blob.device = memory.device(memory.ctx, data);
    }
    return blob;
}
} // namespace

Blob ServerContext::blob(uint32_t field, const uint8_t* inlineData, size_t inlineSize) const {
    if (const detail::BlobLease* lease = call_->blobs.find(field))
        return detail::leaseBlob(*lease);
    return inlineBlobOf(*call_->owner, call_->session->inboundMemory(), inlineData, inlineSize);
}

bool StreamReader::currentBlob(uint32_t field, Blob& out) const {
    const detail::BlobLease* lease = call_->current.leases.find(field);
    if (!lease)
        return false;
    out = detail::leaseBlob(*lease);
    return true;
}

Blob StreamReader::inlineBlob(const uint8_t* data, size_t size) const {
    return inlineBlobOf(*call_->owner, call_->session->inboundMemory(), data, size);
}

detail::TakenItem* StreamReader::takeCurrent() { return call_->session->takeCurrent(*call_); }

namespace detail {

bool takenBlob(const TakenItem* item, uint32_t field, Blob& out) {
    const BlobLease* lease = item->leases.find(field);
    if (!lease)
        return false;
    out = leaseBlob(*lease);
    return true;
}

Blob takenInline(const TakenItem* item, const uint8_t* data, size_t size) {
    return inlineBlobOf(*item->owner, item->session->inboundMemory(), data, size);
}

void releaseTaken(TakenItem* item) noexcept {
    // The blocks go back to the client now, from whichever thread this is.
    std::shared_ptr<Session> session = std::move(item->session);
    session->dropLeases(item->leases, true);
    item->frame.reset();
    item->owner->recycleTaken(item);
}

} // namespace detail

bool StreamWriter::send(const uint8_t* message, size_t size) { return call_->session->streamSend(*call_, message, size); }

// --- Registry ---------------------------------------------------------------------------------------------

void Registry::add(Method method) {
    const bool missing = !method.verifyRequest ||
                         (method.streaming == Streaming::None     ? !method.unary
                          : method.streaming == Streaming::Server ? !method.stream
                          : method.streaming == Streaming::Client ? !method.clientStream
                                                                  : !method.bidi);
    if (missing)
        throw std::logic_error("rpc: method " + method.name + " has no handler");
    const auto at = std::lower_bound(methods_.begin(), methods_.end(), method.id,
                                     [](const Method& m, uint32_t id) { return m.id < id; });
    if (at != methods_.end() && at->id == method.id)
        throw std::logic_error("rpc: method id of " + method.name + " collides with " + at->name);
    methods_.insert(at, std::move(method));
}

const Method* Registry::find(uint32_t id) const noexcept {
    const auto at = std::lower_bound(methods_.begin(), methods_.end(), id,
                                     [](const Method& m, uint32_t wanted) { return m.id < wanted; });
    return at != methods_.end() && at->id == id ? &*at : nullptr;
}

// --- Endpoint ---------------------------------------------------------------------------------------------

Endpoint::Endpoint(EndpointOptions options) : impl_(std::make_unique<detail::EndpointImpl>(std::move(options))) {}

Endpoint::~Endpoint() {
    if (impl_)
        impl_->shutdown(std::chrono::milliseconds(0));
}

Endpoint& Endpoint::add(Service& service) {
    service.bind(impl_->registry);
    return *this;
}

Endpoint& Endpoint::add(std::shared_ptr<Service> service) {
    service->bind(impl_->registry);
    impl_->owned.push_back(std::move(service));
    return *this;
}

Endpoint& Endpoint::attach(http::Server& server, std::string pattern) {
    impl_->attach(server, std::move(pattern));
    return *this;
}

Endpoint& Endpoint::listenLocal(const std::string& path, LocalOptions options) {
    impl_->listenLocal(path, options);
    return *this;
}

void Endpoint::shutdown(std::chrono::milliseconds grace) { impl_->shutdown(grace); }

Endpoint::Stats Endpoint::stats() const {
    Stats out;
    {
        std::lock_guard<std::mutex> lock(impl_->mutex);
        out.sessions = impl_->sessions.size();
        out.inflight = impl_->inflight;
    }
    out.sessionsTotal = impl_->sessionsTotal.load();
    out.calls = impl_->calls.load();
    out.failed = impl_->failed.load();
    out.rejected = impl_->rejected.load();
    out.blobsShared = impl_->blobsShared.load();
    out.blobsInline = impl_->blobsInline.load();
    out.arenas = impl_->arenaAccount->arenas.load();
    out.arenaBytes = impl_->arenaAccount->bytes.load();
    return out;
}

const Registry& Endpoint::registry() const noexcept { return impl_->registry; }

// --- Listener ---------------------------------------------------------------------------------------------

namespace {

http::ServerConfig withMessageLimit(http::ServerConfig config, size_t maxMessage) {
    config.maxWebSocketMessageBytes = std::max(config.maxWebSocketMessageBytes, maxMessage);
    return config;
}

} // namespace

Listener::Listener(http::ServerConfig config, EndpointOptions options, std::string path)
    : server_(withMessageLimit(std::move(config), options.maxMessage())),
      endpoint_(std::move(options)), path_(std::move(path)) {
    endpoint_.attach(server_, path_);
    server_.onGet("/healthz", [this](http::HttpContext& context) {
        const Endpoint::Stats stats = endpoint_.stats();
        context.json(json::Json{{"status", "ok"},
                                {"sessions", static_cast<double>(stats.sessions)},
                                {"inflight", static_cast<double>(stats.inflight)},
                                {"calls", static_cast<double>(stats.calls)},
                                {"failed", static_cast<double>(stats.failed)}});
    });
}

Listener::~Listener() = default;

void Listener::run(std::function<bool()> shouldStop, std::chrono::milliseconds grace) {
    server_.run([this, shouldStop = std::move(shouldStop), grace] {
        if (stop_.load() || (shouldStop && shouldStop())) {
            endpoint_.shutdown(grace);
            return true;
        }
        return false;
    });
}

} // namespace rpc
