// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// librpc server side: services, the registry the generated stubs fill, and the two ways to serve them.
//
//   rpc::Listener     a ready-to-run http::Server with one WebSocket route for RPC (and /healthz):
//
//       rpc::Listener listener(config);
//       listener.add(myCalcService);
//       listener.run(stop);
//
//   rpc::Endpoint     the same route on an http::Server the application already runs, next to its own routes,
//                     and/or a local socket for clients on the same machine (listenLocal):
//
//       http::Server server(config);
//       rpc::Endpoint rpc(rpc::EndpointOptions().authorize(check));
//       rpc.add(myCalcService).attach(server, "/rpc");
//       server.run(stop);
//       rpc.shutdown(std::chrono::seconds(5));
//
//       rpc::Endpoint ipc;                      // no http::Server needed for local clients only
//       ipc.add(myCalcService).listenLocal("/run/calc.sock");      // clients: Client::connect("unix:///run/calc.sock")
//
// Handlers run on the endpoint's own worker threads, never on an I/O thread, so they may block. A call's
// request is verified before the handler sees it. Without an authorize/authenticate hook the route serves
// every method to anyone who can reach it: keep it on a trusted network or put the hooks in.
#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include "bufs/bufs.hpp"
#include "rpc/memory.hpp"
#include "http/server.hpp"
#include "rpc/span.hpp"
#include "rpc/status.hpp"

namespace rpc {

namespace detail {
class Session;
struct ServerCall;
struct EndpointImpl;
class LocalServerLink;
} // namespace detail

/// Who is on the other end, as the application decided.
struct Principal {
    std::string name;            ///< for logs and statistics
    std::shared_ptr<void> data;  ///< anything the application wants back in ServerContext::principal()
};

/// What the WebSocket handshake looked like, for the authorize hook. Valid during the call only.
struct Handshake {
    std::string_view path;           ///< the route, or the socket file of a local connection
    std::string_view remoteAddress;  ///< "host:port"; "pid:<pid>" for a local connection
    /// True for a local-socket connection, whose peer the kernel identified: pid, uid and gid (-1 otherwise).
    /// Headers and route parameters are empty then.
    bool local = false;
    int pid = -1;
    int uid = -1;
    int gid = -1;
    /// A request header by name, "" if absent.
    std::function<std::string_view(std::string_view)> header;
    /// A named part of the route pattern ("/rpc/{tenant}"), "" if absent.
    std::function<std::string_view(std::string_view)> routeParam;
};

/// One call, as the handler sees it. Valid for the duration of the handler.
class ServerContext {
public:
    explicit ServerContext(detail::ServerCall& call) : call_(&call) {}

    uint64_t sessionId() const;
    uint32_t callId() const;
    /// "namespace.Service.Method"
    std::string_view method() const;
    const Principal& principal() const;
    std::string_view remoteAddress() const;
    /// True once the result is no longer wanted: the client cancelled, the deadline passed, the connection
    /// closed or the server is shutting down. A long handler should poll it (a streaming handler learns it
    /// from ServerWriter::write returning false).
    bool cancelled() const;
    /// The call's deadline; time_point::max() if the client set none.
    std::chrono::steady_clock::time_point deadline() const;
    /// The largest message (envelope included) this connection carries, in either direction.
    size_t maxMessage() const;
    /// Runs `hook` (once, on whichever thread notices) when the call is cancelled: the client sent Cancel, its
    /// connection closed, or the server is shutting down past its grace. For work that cannot poll cancelled()
    /// -- a database statement: call its interrupt. The hook must tolerate running while the handler is still
    /// running or just finishing. It runs under a lock that onCancel({}) also takes, so after onCancel({}) returns
    /// the old hook is not running and never will: clear it before the state it refers to goes away. It must be
    /// short and must not wait for anything. Do not call onCancel({}) while holding a lock the hook takes
    /// (the hook runs under librpc's lock, onCancel({}) takes it: that is a lock-order cycle).
    void onCancel(std::function<void()> hook) const;
    /// State an application keeps per connection (a database handle, prepared statements, ...): empty until
    /// setSessionState(). With EndpointOptions::serialPerSession the calls of a session do not overlap, so
    /// a handler may create it lazily without a lock. It is handed to EndpointOptions::onSessionClose when the
    /// connection ends.
    std::shared_ptr<void> sessionState() const;
    void setSessionState(std::shared_ptr<void> state) const;
    template <class T>
    std::shared_ptr<T> sessionState() const {
        return std::static_pointer_cast<T>(sessionState());
    }

private:
    detail::ServerCall* call_;
};

/// The type-erased end of a server stream; ServerWriter<T> is what handlers use.
class StreamWriter {
public:
    explicit StreamWriter(detail::ServerCall& call) : call_(&call) {}

    /// Sends one finished message (bufs::MessageBuilder::finish()) as one stream item. Blocks while the client
    /// has not consumed enough of what was sent (flow control). False: stop producing -- the call is over
    /// (cancelled, deadline, connection gone, or the client stopped reading for stallTimeout).
    bool send(const uint8_t* message, size_t size);
    /// A stream's closing message (the method's `trailer`): sent behind the Ok End, if the stream ends in Ok.
    void setTrailer(const uint8_t* message, size_t size);

private:
    detail::ServerCall* call_;
};

/// Where a streaming handler puts its items.
template <class T>
class ServerWriter {
public:
    explicit ServerWriter(StreamWriter& writer) : writer_(&writer) {}

    /// Builds an item with `fill(T::Builder&)` and sends it. False: stop producing.
    template <class Fill>
    bool write(Fill&& fill) {
        bufs::MessageBuilder builder;
        typename T::Builder root = builder.template initRoot<T>();
        fill(root);
        const auto sealed = builder.finish();
        return writer_->send(sealed.first, sealed.second);
    }

    /// Sends an item built elsewhere.
    bool write(const bufs::Message& item) { return writer_->send(item.data(), item.size()); }
    /// Sends an item that is already a finished message.
    bool write(const uint8_t* message, size_t size) { return writer_->send(message, size); }

private:
    StreamWriter* writer_;
};

/// Where a streaming handler puts its items when the item is not a table (the schema says `stream string`,
/// `stream double`, ...). write() is false once the call is over: stop producing.
template <class T>
class Writer {
public:
    using Sink = bool (*)(void* context, const T& item);
    Writer(Sink sink, void* context) noexcept : sink_(sink), context_(context) {}
    bool write(const T& item) { return sink_(context_, item); }

private:
    Sink sink_;
    void* context_;
};

/// The client's stream, as a handler of a client-streaming or bidirectional method reads it (type-erased).
class StreamReader {
public:
    explicit StreamReader(detail::ServerCall& call) : call_(&call) {}

    /// Waits for the next item the client sent. False when the client has finished its stream, or the call is over
    /// (cancelled, deadline, connection closed). `message` stays valid until the next call. Reading is what lets the
    /// client send more: the window it may fill is replenished as items are read.
    bool next(const uint8_t*& message, size_t& size);

private:
    detail::ServerCall* call_;
};

/// Items of table T from the client:  `while (in.next()) use(in->field());`
template <class T>
class ServerReader {
public:
    explicit ServerReader(StreamReader& in) : in_(&in) {}

    /// False at the end of the client's stream. Throws rpc::Error(InvalidArgument) for an item that is not a T.
    bool next() {
        const uint8_t* message = nullptr;
        size_t size = 0;
        if (!in_->next(message, size))
            return false;
        if (bufs::typeOf(message, size) != T::typeId || !bufs::verify<T>(message, size))
            throw Error(Code::InvalidArgument, "malformed stream item");
        reader_ = bufs::root<T>(message);
        return true;
    }
    const typename T::Reader& reader() const { return reader_; }
    const typename T::Reader* operator->() const { return &reader_; }

private:
    StreamReader* in_;
    typename T::Reader reader_;
};

/// Items that are plain values (the schema says `stream double`, `stream string`, ...): `while (in.next()) use(in.value());`
template <class Wrapper, class V>
class ValueReader {
public:
    using Convert = V (*)(const typename Wrapper::Reader&);
    ValueReader(StreamReader& in, Convert convert) : items_(in), convert_(convert) {}

    bool next() { return items_.next(); }
    /// The current item: scalars by value, strings and vectors as views into the item's message.
    V value() const { return convert_(items_.reader()); }

private:
    ServerReader<Wrapper> items_;
    Convert convert_;
};

enum class Streaming : uint8_t { None, Server, Client, Bidi };

/// What the generated stubs register for each method.
struct Method {
    uint32_t id = 0;                 ///< methodId(name)
    std::string name;                ///< "namespace.Service.Method"
    Streaming streaming = Streaming::None;
    /// The schema marked the method `(inline)`: it is short and never blocks, so on a local connection it runs on
    /// the thread that reads the connection (see EndpointOptions::inlineUnary).
    bool inlineHint = false;
    uint32_t requestType = 0;        ///< the request table's bufs type id
    bool (*verifyRequest)(const void*, size_t) = nullptr;
    /// Unary: fills `response`; a non-Ok status (or a thrown rpc::Error) ends the call without a response.
    std::function<Status(ServerContext&, const uint8_t* request, size_t size, bufs::MessageBuilder& response)> unary;
    /// Server streaming: writes items to `writer`; the returned status ends the stream.
    std::function<Status(ServerContext&, const uint8_t* request, size_t size, StreamWriter& writer)> stream;
    /// Client streaming: reads the client's items from `in`, then fills `response` like a unary method.
    std::function<Status(ServerContext&, const uint8_t* request, size_t size, StreamReader& in, bufs::MessageBuilder& response)>
        clientStream;
    /// Bidirectional: reads `in` and writes `out` as it likes (typically from the same thread, or from two).
    std::function<Status(ServerContext&, const uint8_t* request, size_t size, StreamReader& in, StreamWriter& out)> bidi;
};

/// The methods an endpoint serves.
class Registry {
public:
    /// Throws std::logic_error if the id (or name) is already registered.
    void add(Method method);
    const Method* find(uint32_t id) const noexcept;
    const std::vector<Method>& methods() const noexcept { return methods_; }

private:
    std::vector<Method> methods_;  ///< sorted by id
};

/// What `rpcc` generates a subclass of for every rpc_service. The application derives from the
/// generated class and overrides the methods it implements.
class Service {
public:
    virtual ~Service() = default;
    /// "namespace.Service"
    virtual std::string_view serviceName() const = 0;
    virtual void bind(Registry& registry) = 0;
};

/// A finished call, for the onCall hook (audit logs, metrics). Runs on the worker.
struct CallInfo {
    const Principal& principal;
    std::string_view method;
    const Status& status;
    uint64_t elapsedMicros;
    uint64_t requestBytes;
    uint64_t responseBytes;  ///< payload bytes sent, stream items included
};

class EndpointOptions {
public:
    using AuthorizeResult = std::variant<Principal, int>;

    /// At the handshake: a principal, or an HTTP status to refuse with (401, 403, ...). Status 0 means "no
    /// decision yet": the client may still authenticate with the token of its Hello. Unset: every handshake is
    /// accepted with an anonymous principal.
    EndpointOptions& authorize(std::function<AuthorizeResult(const Handshake&)> fn) {
        authorize_ = std::move(fn);
        return *this;
    }
    /// At the Hello, when authorize left the session without a principal.
    EndpointOptions& authenticate(std::function<std::optional<Principal>(std::string_view token)> fn) {
        authenticate_ = std::move(fn);
        return *this;
    }
    /// A session with no principal after both hooks is refused. Default false.
    EndpointOptions& requireAuth(bool on = true) {
        requireAuth_ = on;
        return *this;
    }
    /// Threads that run handlers. Streaming calls hold one for their whole life. Default 8.
    EndpointOptions& workers(unsigned n) {
        workers_ = n;
        return *this;
    }
    /// Calls that may wait for a worker; beyond it a call ends with Unavailable. Default 1024.
    EndpointOptions& queueCapacity(size_t n) {
        queueCapacity_ = n;
        return *this;
    }
    /// Runs handlers somewhere else (return false when full). Default: an own pool of workers() threads.
    EndpointOptions& executor(std::function<bool(std::function<void()>)> post) {
        executor_ = std::move(post);
        return *this;
    }
    /// The calls of one session run one at a time, in the order they arrived (a streaming call holds the session
    /// until it ends). Needed by an application whose session is a stateful conversation (a transaction, say);
    /// Cancel still reaches a running call. Default false: calls of a session run concurrently.
    EndpointOptions& serialPerSession(bool on = true) {
        serialPerSession_ = on;
        return *this;
    }
    /// Local connections only: run short unary handlers directly on the thread that reads the connection
    /// instead of handing them to a worker, which saves a thread hop (several microseconds). A handler that
    /// blocks then delays the other calls of that connection; streaming calls always use a worker.
    EndpointOptions& inlineUnary(bool on = true) {
        inlineUnary_ = on;
        return *this;
    }
    /// Local connections: how long a thread polls for a message before it sleeps on the futex. Default 20 us.
    EndpointOptions& ipcSpin(std::chrono::microseconds t) {
        ipcSpin_ = t;
        return *this;
    }
    /// How long an idle worker polls for the next call before it sleeps. A sleeping worker costs a futex wake-up
    /// (tens of microseconds) when a call arrives; a polling one takes it at once, but a polling thread needs a core of its
    /// own: with fewer cores than busy threads it makes things worse. Default 0 (sleep at once); try 20 us on a
    /// machine with spare cores.
    EndpointOptions& workerSpin(std::chrono::microseconds t) {
        workerSpin_ = t;
        return *this;
    }
    /// Called, on a worker, when a session ends, with the state its handlers set (see ServerContext).
    EndpointOptions& onSessionClose(std::function<void(uint64_t sessionId, std::shared_ptr<void> state)> fn) {
        onSessionClose_ = std::move(fn);
        return *this;
    }
    EndpointOptions& maxSessions(size_t n) {
        maxSessions_ = n;
        return *this;
    }
    /// Calls one session may have running or queued; beyond it a call ends with ResourceExhausted. Default 64.
    EndpointOptions& maxInflightPerSession(size_t n) {
        maxInflight_ = n;
        return *this;
    }
    /// Bytes of stream items a client may leave unread before the server waits. Default 1 MiB; it is cut to
    /// half of the http::Server's maxWebSocketQueuedBytes, which is what keeps the connection from being
    /// closed for queueing too much.
    EndpointOptions& window(size_t bytes) {
        window_ = bytes;
        return *this;
    }
    /// Where received frames (requests, stream items) land; see rpc/memory.hpp. The one copy out of the socket or
    /// ring can go straight into memory a GPU can read, e.g. a PinnedPool registered with CUDA. Default: librpc's own
    /// pool. The memory must outlive the endpoint and every message handed to a handler.
    EndpointOptions& inboundMemory(InboundMemory memory) {
        inboundMemory_ = memory;
        return *this;
    }
    /// Largest WebSocket message either way; 0 = the http::Server's maxWebSocketMessageBytes.
    EndpointOptions& maxMessage(size_t bytes) {
        maxMessage_ = bytes;
        return *this;
    }
    size_t maxMessage() const { return maxMessage_; }
    /// A connection that sends nothing (clients ping) for this long is closed. Default 5 minutes.
    EndpointOptions& idleTimeout(std::chrono::seconds t) {
        idleTimeout_ = t;
        return *this;
    }
    /// How long a stream may wait for a client that does not read before the session is cut. Default 30 s.
    EndpointOptions& stallTimeout(std::chrono::seconds t) {
        stallTimeout_ = t;
        return *this;
    }
    /// Without it a handler's exception reaches the client as Internal("internal error"); with it, as its what().
    EndpointOptions& exposeExceptions(bool on = true) {
        exposeExceptions_ = on;
        return *this;
    }
    EndpointOptions& serverName(std::string name) {
        serverName_ = std::move(name);
        return *this;
    }
    EndpointOptions& onCall(std::function<void(const CallInfo&)> fn) {
        onCall_ = std::move(fn);
        return *this;
    }

private:
    friend struct detail::EndpointImpl;
    friend class detail::Session;
    friend class detail::LocalServerLink;
    std::function<AuthorizeResult(const Handshake&)> authorize_;
    std::function<std::optional<Principal>(std::string_view)> authenticate_;
    bool requireAuth_ = false;
    unsigned workers_ = 8;
    size_t queueCapacity_ = 1024;
    std::function<bool(std::function<void()>)> executor_;
    size_t maxSessions_ = 1024;
    size_t maxInflight_ = 64;
    bool serialPerSession_ = false;
    bool inlineUnary_ = false;
    std::chrono::microseconds ipcSpin_{20};
    std::chrono::microseconds workerSpin_{0};
    std::function<void(uint64_t, std::shared_ptr<void>)> onSessionClose_;
    size_t window_ = 1 << 20;
    InboundMemory inboundMemory_;
    size_t maxMessage_ = 0;
    std::chrono::seconds idleTimeout_{300};
    std::chrono::seconds stallTimeout_{30};
    bool exposeExceptions_ = false;
    std::string serverName_ = "librpc/0.1";
    std::function<void(const CallInfo&)> onCall_;
};

/// A local socket: for clients on the same machine, which reach the server through shared memory.
struct LocalOptions {
    /// File mode of the socket; the default admits the owner only. Widen it (and authorize by Handshake::uid)
    /// to serve other users.
    unsigned mode = 0600;
};

/// The RPC route of an http::Server, and/or a local socket.
class Endpoint {
public:
    explicit Endpoint(EndpointOptions options = {});
    ~Endpoint();
    Endpoint(const Endpoint&) = delete;
    Endpoint& operator=(const Endpoint&) = delete;

    /// Serves a service. Before attach(). The service must outlive shutdown().
    Endpoint& add(Service& service);
    Endpoint& add(std::shared_ptr<Service> service);

    /// Registers the WebSocket route (`pattern` may have {parameters}). Before server.run().
    Endpoint& attach(http::Server& server, std::string pattern = "/rpc");

    /// Listens on a Unix socket at `path` (an old socket file is replaced; one a live server uses is an error).
    /// A client connects with Client::connect("unix://<path>"): the server creates an anonymous shared-memory
    /// channel for it and passes it over the socket, and from then on frames travel through two rings in that
    /// memory with futex wake-ups: no system call per message while either side is busy, and no copy beyond one
    /// per direction. The kernel tells the authorize hook who the peer is (Handshake::pid/uid/gid). Works
    /// without an http::Server. Call before serving; may be combined with attach().
    Endpoint& listenLocal(const std::string& path, LocalOptions options = {});

    /// Stops taking calls (new ones end Unavailable), tells the clients, lets running calls finish for up to
    /// `grace`, cancels the rest, closes the sessions and joins the workers. Call it while the http::Server
    /// still runs. Idempotent; the destructor calls it with no grace.
    void shutdown(std::chrono::milliseconds grace = std::chrono::seconds(5));

    struct Stats {
        size_t sessions = 0;        ///< open now
        uint64_t sessionsTotal = 0;
        size_t inflight = 0;        ///< calls running or queued now
        uint64_t calls = 0;         ///< calls started
        uint64_t failed = 0;        ///< ended with a status other than Ok
        uint64_t rejected = 0;      ///< refused before a handler ran (unknown method, bad request, overload)
    };
    Stats stats() const;

    const Registry& registry() const noexcept;

private:
    std::unique_ptr<detail::EndpointImpl> impl_;
};

/// A server that is ready to run: an http::Server with the RPC route and a /healthz route.
class Listener {
public:
    explicit Listener(http::ServerConfig config, EndpointOptions options = {}, std::string path = "/rpc");
    ~Listener();

    Listener& add(Service& service) {
        endpoint_.add(service);
        return *this;
    }
    Listener& add(std::shared_ptr<Service> service) {
        endpoint_.add(std::move(service));
        return *this;
    }
    /// Also serves local clients on a Unix socket (see Endpoint::listenLocal).
    Listener& listenLocal(const std::string& path, LocalOptions options = {}) {
        endpoint_.listenLocal(path, options);
        return *this;
    }

    /// For the application's own routes; add them before run().
    http::Server& http() { return server_; }
    Endpoint& endpoint() { return endpoint_; }
    const std::string& path() const { return path_; }

    /// Serves until `shouldStop` (polled) returns true or stop() is called, then shuts the endpoint down
    /// with `grace` and returns.
    void run(std::function<bool()> shouldStop = {}, std::chrono::milliseconds grace = std::chrono::seconds(5));
    /// From any thread (or a signal handler: it only sets a flag).
    void stop() noexcept { stop_.store(true); }

private:
    http::Server server_;
    Endpoint endpoint_;
    std::string path_;
    std::atomic<bool> stop_{false};
};

} // namespace rpc
