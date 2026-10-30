// librpc integration tests: real servers on loopback, real clients.
#include <arpa/inet.h>
#include <signal.h>
#include <spawn.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstring>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>

#include <fcntl.h>
#include <poll.h>
#include <sys/mman.h>
#include <sys/syscall.h>

#include "calc_service.hpp"
#include "ipc_common.hpp"
#include "kitchen_service.hpp"
#include "bufs/fd.hpp"
#include "bufs/local.hpp"
#include "bufs/shm.hpp"
#include "http/websocket_client.hpp"
#include "rpc/detail/frame.hpp"

using namespace std::chrono_literals;
using namespace sample::calc;

namespace {

int g_failures = 0;
int g_checks = 0;
bool g_local = false;  // run the tests over a local socket instead of a WebSocket

#define CHECK(cond)                                                                        \
    do {                                                                                   \
        ++g_checks;                                                                        \
        if (!(cond)) {                                                                     \
            ++g_failures;                                                                  \
            std::fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__, __LINE__, #cond); \
        }                                                                                  \
    } while (0)

// Runs `fn`, which must throw rpc::Error with `code`.
template <class Fn>
bool throwsCode(rpc::Code code, Fn&& fn) {
    try {
        fn();
    } catch (const rpc::Error& error) {
        if (error.code() == code)
            return true;
        std::fprintf(stderr, "  expected %s, got %s\n", rpc::codeName(code), error.what());
        return false;
    }
    std::fprintf(stderr, "  expected %s, nothing thrown\n", rpc::codeName(code));
    return false;
}

uint16_t freePort() {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    ::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof addr);
    socklen_t len = sizeof addr;
    ::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len);
    const uint16_t port = ntohs(addr.sin_port);
    ::close(fd);
    return port;
}

// A Calc + Kitchens server on its own port, running on its own thread.
class TestServer {
public:
    explicit TestServer(rpc::EndpointOptions options = {}, std::chrono::milliseconds grace = 5s) : port_(freePort()) {
        options.onCall([this](const rpc::CallInfo& call) {
            std::lock_guard<std::mutex> lock(mutex_);
            finished_.push_back({std::string(call.method), call.status.code()});
            changed_.notify_all();
        });
        http::ServerConfig config;
        config.host = "127.0.0.1";
        config.port = port_;
        config.ioThreads = 2;
        listener_ = std::make_unique<rpc::Listener>(config, std::move(options));
        listener_->add(calc_).add(kitchens_).add(natural_).add(blobs_);
        localPath_ = "/tmp/librpc-test-" + std::to_string(::getpid()) + "-" + std::to_string(port_) + ".sock";
        listener_->listenLocal(localPath_);
        thread_ = std::thread([this, grace] { listener_->run({}, grace); });
        waitListening();
    }
    ~TestServer() { stop(); }

    void stop() {
        if (thread_.joinable()) {
            listener_->stop();
            thread_.join();
        }
    }

    // Until the port accepts connections.
    void waitListening() const {
        for (int attempt = 0; attempt < 200; ++attempt) {
            const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
            sockaddr_in addr{};
            addr.sin_family = AF_INET;
            addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
            addr.sin_port = htons(port_);
            const bool up = ::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof addr) == 0;
            ::close(fd);
            if (up)
                return;
            std::this_thread::sleep_for(25ms);
        }
    }

    std::string url() const {
        return g_local ? "unix://" + localPath_ : "ws://127.0.0.1:" + std::to_string(port_) + "/rpc";
    }
    const std::string& localPath() const { return localPath_; }
    uint16_t port() const { return port_; }

    rpc::Client connect(rpc::ClientOptions options = {}) {
        for (int attempt = 0;; ++attempt) {
            try {
                return rpc::Client::connect(url(), options);
            } catch (const rpc::Error& error) {
                if (error.code() != rpc::Code::Unavailable || attempt > 100)
                    throw;
                std::this_thread::sleep_for(50ms);
            }
        }
    }

    // Waits until a call of `method` ended with `code`.
    bool waitFor(const std::string& method, rpc::Code code, std::chrono::milliseconds timeout = 3s) {
        std::unique_lock<std::mutex> lock(mutex_);
        return changed_.wait_for(lock, timeout, [&] {
            for (const auto& entry : finished_)
                if (entry.first == method && entry.second == code)
                    return true;
            return false;
        });
    }

    rpc::Listener& listener() { return *listener_; }
    test::NaturalImpl& natural() { return natural_; }
    test::BlobsImpl& blobs() { return blobs_; }

private:
    uint16_t port_;
    std::string localPath_;
    CalcImpl calc_;
    test::KitchensImpl kitchens_;
    test::NaturalImpl natural_;
    test::BlobsImpl blobs_;
    std::unique_ptr<rpc::Listener> listener_;
    std::thread thread_;
    std::mutex mutex_;
    std::condition_variable changed_;
    std::vector<std::pair<std::string, rpc::Code>> finished_;
};

void testUnary() {
    std::puts("unary calls");
    TestServer server;
    CalcClient calc(server.connect());

    CHECK(calc.Add(6, 7) == 13);
    CHECK(calc.Sub(1, 0.5) == 0.5);
    const std::vector<double> values{1, 2, 3.5};
    CHECK(calc.Sum(values) == 6.5);
    calc.Ping();

    // strings and vectors come back as views into the received message
    auto echo = calc.Echo("hello");
    CHECK(echo->result() == "hello");

    // a struct and a vector of structs as parameters
    const Point points[] = {{3, 0}, {3, 4}};
    CHECK(calc.PathLength({0, 0}, points) == 7);

    // a status from the handler
    CHECK(throwsCode(rpc::Code::InvalidArgument, [&] { calc.Div(1, 0); }));
    CHECK(server.waitFor("sample.calc.Calc.Div", rpc::Code::InvalidArgument));

    // a large message in both directions
    const std::string big(900 * 1024, 'x');
    CHECK(calc.Echo(big)->result().size() == big.size());
}

void testBadRequests() {
    std::puts("bad requests");
    TestServer server;
    rpc::Client client = server.connect();

    bufs::MessageBuilder builder;
    builder.initRoot<test::Pick>().setWhich(1);
    const auto pick = builder.finish();

    // a method nobody registered
    CHECK(throwsCode(rpc::Code::Unimplemented, [&] { client.call(0x12345678u, pick.first, pick.second); }));
    // the right method, the wrong request type
    CHECK(throwsCode(rpc::Code::InvalidArgument, [&] { client.call(CalcMethods::Add, pick.first, pick.second); }));
    // the right type id, a table that runs past the end of the message
    alignas(8) uint8_t forged[16] = {};
    const uint32_t size = 16, type = CalcAddRequest::typeId;
    std::memcpy(forged, &size, 4);
    std::memcpy(forged + 4, &type, 4);
    forged[8] = 0xff;
    forged[9] = 0xff;
    CHECK(throwsCode(rpc::Code::InvalidArgument, [&] { client.call(CalcMethods::Add, forged, sizeof forged); }));
    // the connection survives all of that
    CalcClient calc(client);
    CHECK(calc.Echo("still here")->result() == "still here");
}

void testStreaming() {
    std::puts("server streaming and flow control");
    // A window of 64 KiB and 16 KiB items: the server must wait for the client over and over.
    TestServer server(rpc::EndpointOptions().window(64 * 1024));
    CalcClient calc(server.connect());

    int expected = 1;
    for (auto stream = calc.Count(1, 300, 1, 0, 16 * 1024); stream.next();) {
        CHECK(stream->n() == expected);
        CHECK(stream->payload().size() == 16 * 1024);
        ++expected;
        if (expected % 50 == 0)
            std::this_thread::sleep_for(20ms);  // a reader that falls behind
    }
    CHECK(expected == 301);

    // the default step (a non-zero default in the schema) and counting down
    int count = 0;
    for (auto stream = calc.Count(1, 3); stream.next();)
        ++count;
    CHECK(count == 3);
    int last = 0;
    for (auto stream = calc.Count(5, 1, -2); stream.next();)
        last = stream->n();
    CHECK(last == 1);

    // a stream that ends early on the client: the server notices, and the connection stays usable
    {
        auto stream = calc.Count(1, 100000, 1, 5, 1000);
        CHECK(stream.next());
        CHECK(stream.next());
    }
    CHECK(server.waitFor("sample.calc.Calc.Count", rpc::Code::Cancelled));
    // ... and its unread items did not wedge the window
    count = 0;
    for (auto stream = calc.Count(1, 200, 1, 0, 16 * 1024); stream.next();)
        ++count;
    CHECK(count == 200);
}

struct PinCount {
    std::atomic<int> registered{0}, unregistered{0};
};

void testInboundMemory() {
    std::puts("received frames in caller-supplied memory");
    // The pools outlive the server and client: they must outlive every message.
    PinCount serverPins, clientPins;
    const auto makePool = [](PinCount& pins) {
        // The fake device address is the block's own plus 1, so device() can be told from the host address.
        return std::make_unique<rpc::PinnedPool>(rpc::DeviceRegistration{
            [](void* ctx, void* p, size_t) -> void* { static_cast<PinCount*>(ctx)->registered++; return static_cast<uint8_t*>(p) + 1; },
            [](void* ctx, void*, size_t) { static_cast<PinCount*>(ctx)->unregistered++; }, &pins});
    };
    auto serverPool = makePool(serverPins), clientPool = makePool(clientPins);
    {
        TestServer server(rpc::EndpointOptions().window(64 * 1024).inboundMemory(serverPool->memory()));
        rpc::ClientOptions options;
        options.inboundMemory = clientPool->memory();
        CalcClient calc(server.connect(options));
        for (int round = 0; round < 3; ++round) {
            auto up = calc.Upload("pinned");
            std::vector<uint8_t> data(20 * 1024, 5);
            for (int i = 0; i < 10; ++i)
                CHECK(up.write([&](Chunk::Builder& c) { c.setData(data.data(), data.size()); }));
            CHECK(up.finish()->result() == 10u * 20 * 1024);
        }
        auto total = calc.Total();
        for (int i = 1; i <= 10; ++i)
            CHECK(total.write([&](CalcTotalItem::Builder& b) { b.setItem(i); }));
        CHECK(total.finish()->result() == 55);
    }
    CHECK(serverPool->blocks() > 0 && clientPool->blocks() > 0);
    {
        // device(): the registration's address for any byte of a block, nullptr elsewhere
        rpc::PinnedPool pool({[](void*, void* p, size_t) -> void* { return static_cast<uint8_t*>(p) + 1; }, [](void*, void*, size_t) {}, nullptr});
        const rpc::InboundMemory memory = pool.memory();
        auto* a = static_cast<uint8_t*>(memory.allocate(memory.ctx, 5000));
        auto* b = static_cast<uint8_t*>(memory.allocate(memory.ctx, 100));
        CHECK(a && b);
        CHECK(pool.device(a) == a + 1 && pool.device(a + 4999) == a + 5000 && pool.device(b + 99) == b + 100);
        CHECK(memory.device(memory.ctx, a + 8) == a + 9);
        int outside = 0;
        CHECK(pool.device(&outside) == nullptr);
        memory.release(memory.ctx, a, 5000);
        memory.release(memory.ctx, b, 100);
        rpc::PinnedPool plain;  // no registration: nothing is device-visible
        auto* c = static_cast<uint8_t*>(plain.memory().allocate(plain.memory().ctx, 64));
        CHECK(plain.device(c) == nullptr);
        plain.memory().release(plain.memory().ctx, c, 64);
    }
    CHECK(serverPool->inUse() == 0 && clientPool->inUse() == 0);
    CHECK(serverPins.registered == static_cast<int>(serverPool->blocks()));
    // grow-only: three rounds of the same traffic reuse blocks, they do not register new ones each time
    CHECK(serverPool->blocks() < 40);
    const int serverBlocks = static_cast<int>(serverPool->blocks()), clientBlocks = static_cast<int>(clientPool->blocks());
    serverPool.reset();
    clientPool.reset();
    CHECK(serverPins.unregistered == serverBlocks && clientPins.unregistered == clientBlocks);
    // allocate() refusing (nullptr) falls back to librpc's own pool
    {
        rpc::InboundMemory refuse;
        refuse.allocate = [](void*, size_t) -> void* { return nullptr; };
        refuse.release = [](void*, void*, size_t) { std::abort(); };
        TestServer server(rpc::EndpointOptions().inboundMemory(refuse));
        CalcClient calc(server.connect());
        auto total = calc.Total();
        total.write([&](CalcTotalItem::Builder& b) { b.setItem(4); });
        CHECK(total.finish()->result() == 4);
    }
}

void testStreamsFromTheClient() {
    std::puts("client streaming and bidirectional calls");
    // A window of 64 KiB: what the client uploads is held back until the server has read it.
    TestServer server(rpc::EndpointOptions().window(64 * 1024));
    CalcClient calc(server.connect());

    // client streaming: many items, then the answer
    {
        auto total = calc.Total();
        for (int i = 1; i <= 100; ++i)
            CHECK(total.write([&](CalcTotalItem::Builder& b) { b.setItem(i); }));
        auto answer = total.finish();
        CHECK(answer->result() == 5050);
    }
    // ... none at all
    CHECK(calc.Total().finish()->result() == 0);
    // ... far more than the window holds (6.4 MB through 64 KiB), with a parameter in the call itself
    {
        auto up = calc.Upload("big");
        std::vector<uint8_t> data(16 * 1024, 7);
        for (int i = 0; i < 400; ++i)
            CHECK(up.write([&](Chunk::Builder& c) { c.setData(data.data(), data.size()); }));
        CHECK(up.finish()->result() == 400u * 16 * 1024);
    }
    // a small frame, then one too big for the rest of the window: the server credits the small one once it has read it
    {
        auto up = calc.Upload("mixed");
        std::vector<uint8_t> small(100, 1), large(60 * 1024, 2);
        CHECK(up.write([&](Chunk::Builder& c) { c.setData(small.data(), small.size()); }));
        CHECK(up.write([&](Chunk::Builder& c) { c.setData(large.data(), large.size()); }));
        CHECK(up.finish()->result() == small.size() + large.size());
    }
    // bidirectional, one thread: each answer follows its question
    {
        auto run = calc.Running();
        double expected = 0;
        for (int i = 1; i <= 50; ++i) {
            CHECK(run.write([&](CalcRunningItem::Builder& b) { b.setItem(i); }));
            CHECK(run.next());
            expected += i;
            CHECK(run->result() == expected);
        }
        run.closeSend();
        CHECK(!run.next());  // the server ends its stream when ours ends
    }
    // bidirectional, a writer thread and a reader thread, both directions through the small window
    {
        auto chat = calc.Chat(">> ");
        const std::string pad(1000, 'x');
        std::thread writer([&] {
            for (int i = 0; i < 2000; ++i)
                if (!chat.write([&](Line::Builder& line) { line.setText("message " + std::to_string(i) + pad); }))
                    break;
            chat.closeSend();
        });
        int n = 0;
        bool inOrder = true;
        while (chat.next()) {
            inOrder = inOrder && chat->text() == ">> message " + std::to_string(n) + pad;
            ++n;
        }
        writer.join();
        CHECK(n == 2000);
        CHECK(inOrder);
    }
    // cancelling an upload wakes the handler that is waiting for the next item
    {
        auto total = calc.Total();
        total.write([](CalcTotalItem::Builder& b) { b.setItem(1); });
        std::this_thread::sleep_for(50ms);
        total.cancel();
        CHECK(server.waitFor("sample.calc.Calc.Total", rpc::Code::Cancelled));
    }
    // the connection is fine after all that
    CHECK(calc.Add(1, 2) == 3);
    // an upload abandoned without finish() (the call object is dropped): the server cancels and the window is not leaked
    for (int round = 0; round < 3; ++round) {
        auto up = calc.Upload("abandoned");
        std::vector<uint8_t> data(16 * 1024, 1);
        for (int i = 0; i < 3; ++i)
            up.write([&](Chunk::Builder& c) { c.setData(data.data(), data.size()); });
    }
    {
        auto up = calc.Upload("after");
        std::vector<uint8_t> data(16 * 1024, 1);
        for (int i = 0; i < 100; ++i)
            CHECK(up.write([&](Chunk::Builder& c) { c.setData(data.data(), data.size()); }));
        CHECK(up.finish()->result() == 100u * 16 * 1024);
    }
}

void testDeadlinesAndCancel() {
    std::puts("deadlines");
    TestServer server;
    CalcClient calc(server.connect());
    rpc::CallOptions options;
    options.timeout = 150ms;
    const auto start = std::chrono::steady_clock::now();
    CHECK(throwsCode(rpc::Code::DeadlineExceeded, [&] { calc.Sleep(5000, options); }));
    CHECK(std::chrono::steady_clock::now() - start < 2s);
    // the server's handler was woken by the cancel or the deadline
    CHECK(server.waitFor("sample.calc.Calc.Sleep", rpc::Code::Cancelled) ||
          server.waitFor("sample.calc.Calc.Sleep", rpc::Code::DeadlineExceeded));

    // a stream with a deadline
    options.timeout = 200ms;
    CHECK(throwsCode(rpc::Code::DeadlineExceeded, [&] {
        for (auto stream = calc.Count(1, 1000000, 1, 20, 0, options); stream.next();) {
        }
    }));
    // a call that fits its deadline
    options.timeout = 5s;
    CHECK(calc.Sleep(10, options) == 10);
}

void testConcurrency() {
    std::puts("concurrent callers");
    TestServer server;
    rpc::Client shared = server.connect();
    std::atomic<int> wrong{0};
    std::vector<std::thread> threads;
    for (int t = 0; t < 8; ++t) {
        threads.emplace_back([&, t] {
            CalcClient calc(shared);
            for (int i = 0; i < 200; ++i) {
                const double a = t * 1000 + i;
                if (calc.Add(a, 1) != a + 1)
                    ++wrong;
                if (i % 50 == 0) {
                    int n = 0;
                    for (auto s = calc.Count(1, 20); s.next();)
                        ++n;
                    if (n != 20)
                        ++wrong;
                }
            }
        });
    }
    for (auto& thread : threads)
        thread.join();
    CHECK(wrong == 0);
    CHECK(server.listener().endpoint().stats().calls >= 1600);
}

void testAuth() {
    std::puts("authentication");
    rpc::EndpointOptions options;
    options.requireAuth().authenticate([](std::string_view token) -> std::optional<rpc::Principal> {
        if (token == "sesame")
            return rpc::Principal{"alice", nullptr};
        return std::nullopt;
    });
    TestServer server(std::move(options));
    rpc::ClientOptions bad;
    bad.token = "wrong";
    CHECK(throwsCode(rpc::Code::Unauthenticated, [&] { rpc::Client::connect(server.url(), bad); }));
    CHECK(throwsCode(rpc::Code::Unauthenticated, [&] { rpc::Client::connect(server.url()); }));
    rpc::ClientOptions good;
    good.token = "sesame";
    CalcClient calc(server.connect(good));
    CHECK(calc.Echo("hi")->result() == "hi");

    if (g_local)
        return;
    // the handshake hook: a header decides before any frame is exchanged
    rpc::EndpointOptions byHeader;
    byHeader.authorize([](const rpc::Handshake& handshake) -> rpc::EndpointOptions::AuthorizeResult {
        if (handshake.header("authorization") == "Bearer ok")
            return rpc::Principal{"bob", nullptr};
        return 401;
    });
    TestServer second(std::move(byHeader));
    CHECK(throwsCode(rpc::Code::Unauthenticated, [&] { rpc::Client::connect(second.url()); }));
    rpc::ClientOptions headers;
    headers.headers = {{"Authorization", "Bearer ok"}};
    CalcClient allowed(second.connect(headers));
    CHECK(allowed.Echo("hi")->result() == "hi");
}

class Misbehaving : public CalcService {
public:
    double Add(rpc::ServerContext&, double, double) override { throw std::runtime_error("secret internals"); }
    double Sum(rpc::ServerContext&, bufs::Vector<double>) override { throw rpc::Error(rpc::Code::NotFound, "no such sum"); }
    // Echo stays Unimplemented
};

void testHandlerErrors() {
    if (g_local)
        return;
    std::puts("handler errors");
    for (bool expose : {false, true}) {
        http::ServerConfig config;
        config.host = "127.0.0.1";
        config.port = freePort();
        config.ioThreads = 1;
        Misbehaving service;
        rpc::Listener listener(config, rpc::EndpointOptions().exposeExceptions(expose));
        listener.add(service);
        std::thread thread([&] { listener.run(); });
        CalcClient calc([&] {
            for (int i = 0;; ++i) {
                try {
                    return rpc::Client::connect("ws://127.0.0.1:" + std::to_string(config.port) + "/rpc");
                } catch (const rpc::Error&) {
                    if (i > 100)
                        throw;
                    std::this_thread::sleep_for(50ms);
                }
            }
        }());
        try {
            calc.Add(1, 2);
            CHECK(false);
        } catch (const rpc::Error& error) {
            CHECK(error.code() == rpc::Code::Internal);
            const std::string what = error.status().message();
            CHECK((what.find("secret internals") != std::string::npos) == expose);
        }
        CHECK(throwsCode(rpc::Code::NotFound, [&] { calc.Sum(std::vector<double>{}); }));
        CHECK(throwsCode(rpc::Code::Unimplemented, [&] { calc.Echo(""); }));
        listener.stop();
        thread.join();
    }
}

void testKitchen() {
    std::puts("every field type");
    TestServer server;
    test::KitchensClient kitchens(server.connect());

    // server-built, read by the client
    auto canned = kitchens.Canned([](test::Pick::Builder& r) { r.setWhich(0); });
    CHECK(test::checkKitchen(canned.reader()).empty());
    if (!test::checkKitchen(canned.reader()).empty())
        std::fprintf(stderr, "  %s\n", test::checkKitchen(canned.reader()).c_str());
    auto defaults = kitchens.Canned([](test::Pick::Builder& r) { r.setWhich(1); });
    CHECK(test::checkKitchen(defaults.reader()).empty());

    // client-built, checked by the server
    auto verdict = kitchens.Check([](test::Kitchen::Builder& k) { test::fillReference(k); });
    CHECK(verdict->ok());
    if (!verdict->ok())
        std::fprintf(stderr, "  server says: %.*s\n", static_cast<int>(verdict->message().size()), verdict->message().data());
    auto empty = kitchens.Check([](test::Kitchen::Builder& k) { k.setTag(1); });
    CHECK(empty->ok());
}

void testNatural() {
    std::puts("natural-form methods of every type");
    TestServer server;
    test::NaturalClient natural(server.connect());

    const test::Vec v = test::referenceVec(0);
    const std::vector<int32_t> nums{1, 2, 3};
    const std::string_view names[] = {"a", "b"};
    const bool flags[] = {true, false, true};
    const test::Vec vecs[] = {test::referenceVec(1), test::referenceVec(2)};
    const rpc::Fill<test::Inner> inner = [](test::Inner::Builder& i) {
        i.setN(7);
        i.setS("x");
    };
    auto described = natural.Describe(true, -5, 1.5f, test::Color::Green, "hi", v, nums, names, flags, vecs, inner);
    CHECK(described->result() == "b=1 big=-5 f=1.5 c=5 s=hi v=1.5,2.5,-3.5,3,1,-5 nums=1,2,3 names=a|b flags=101 vecs=2 inner=7:x d=2.5");
    // the default of the last parameter can be replaced
    CHECK(natural.Describe(false, 0, 0, test::Color::Red, "", v, {}, {}, {}, {}, inner, 4.25)->result().find("d=4.25") != std::string::npos);

    auto squares = natural.Squares(4);
    CHECK(squares->result().size() == 4 && squares->result()[3] == 16);
    auto words = natural.Words();
    CHECK(words->result().size() == 3 && words->result()[0] == "alpha" && words->result()[1].empty());
    CHECK(natural.Flip(true) == false);
    CHECK(natural.Origin().big == test::referenceVec(0).big);
    auto made = natural.Make("widget");
    CHECK(made->n() == 6 && made->s() == "widget");
    natural.Nothing();

    int expected = 3;
    for (auto s = natural.Countdown(3); s.next();) {
        CHECK(s.value() == "t-minus " + std::to_string(expected));
        --expected;
    }
    CHECK(expected == 0);
    int colors = 0;
    for (auto s = natural.Colors(); s.next();)
        colors += static_cast<int>(s.value());
    CHECK(colors == 1 + 5 - 2);

    // a stream with a trailer: the closing message arrives after the last item
    {
        int items = 0;
        auto s = natural.Tally(5);
        CHECK(!s.trailer().ok());  // nothing yet
        while (s.next())
            items += s->n() == items + 1;
        CHECK(items == 5);
        CHECK(s.trailer().ok() && s.trailer().message() == "5 items");
    }
    // an application detail travels with the status
    try {
        natural.Fail(0x1234);
        CHECK(false);
    } catch (const rpc::Error& error) {
        CHECK(error.code() == rpc::Code::Aborted && error.status().detail() == 0x1234 && error.status().message() == "failed on purpose");
    }
    // a hook runs when the call is cancelled, so a handler that cannot poll is woken
    {
        rpc::Client other = server.connect();
        test::NaturalClient hanging(other);
        std::atomic<int> code{-1};
        std::thread caller([&] {
            try {
                hanging.Hang();
            } catch (const rpc::Error& error) {
                code = static_cast<int>(error.code());
            }
        });
        std::this_thread::sleep_for(150ms);
        other.cancelAll();
        caller.join();
        CHECK(code == static_cast<int>(rpc::Code::Cancelled));
        for (int i = 0; i < 100 && server.natural().hooksRun.load() == 0; ++i)
            std::this_thread::sleep_for(20ms);
        CHECK(server.natural().hooksRun.load() == 1);
        CHECK(other.connected());  // the connection survives
        CHECK(test::NaturalClient(other).Flip(false) == true);
    }
}

void testStall() {
    std::puts("a client that stops reading");
    TestServer server(rpc::EndpointOptions().window(64 * 1024).stallTimeout(1s));
    CalcClient calc(server.connect());
    auto stream = calc.Count(1, 100000, 1, 0, 16 * 1024);
    std::this_thread::sleep_for(2500ms);  // never reading: the window fills, the server gives up on us
    CHECK(throwsCode(rpc::Code::Unavailable, [&] {
        while (stream.next()) {
        }
    }));
    CHECK(!calc.connection().connected());
}

void testShutdown() {
    std::puts("graceful shutdown");
    auto server = std::make_unique<TestServer>(rpc::EndpointOptions(), 300ms);
    CalcClient calc(server->connect());
    std::atomic<int> code{-1};
    std::thread caller([&] {
        try {
            calc.Sleep(30000);
        } catch (const rpc::Error& error) {
            code = static_cast<int>(error.code());
        }
    });
    CHECK(server->waitFor("", rpc::Code::Ok, 300ms) == false);  // let the call start
    std::this_thread::sleep_for(200ms);
    const auto start = std::chrono::steady_clock::now();
    server->stop();
    caller.join();
    CHECK(std::chrono::steady_clock::now() - start < 5s);
    CHECK(code == static_cast<int>(rpc::Code::Cancelled) || code == static_cast<int>(rpc::Code::Unavailable));
    // the client learns that the connection is gone, and new calls fail fast
    CHECK(throwsCode(rpc::Code::Unavailable, [&] { calc.Echo(""); }));
    CHECK(throwsCode(rpc::Code::Unavailable, [&] { rpc::Client::connect(server->url()); }));
}

// Only on a local socket: who the peer is, a refusal, a server in another process, a dead client.
void testLocalSocket() {
    std::puts("local socket specifics");
    // The kernel tells the hook who is on the other end.
    std::atomic<int> seenUid{-1};
    rpc::EndpointOptions options;
    options.authorize([&](const rpc::Handshake& handshake) -> rpc::EndpointOptions::AuthorizeResult {
        if (!handshake.local)
            return 0;
        seenUid = handshake.uid;
        if (handshake.uid == static_cast<int>(::getuid()))
            return rpc::Principal{"me", nullptr};
        return 403;
    });
    {
        TestServer server(std::move(options));
        rpc::Client client = rpc::Client::connect("unix://" + server.localPath());
        CHECK(seenUid == static_cast<int>(::getuid()));
        CHECK(CalcClient(client).Add(1, 2) == 3);
        struct stat st {};
        CHECK(::stat(server.localPath().c_str(), &st) == 0 && (st.st_mode & 0777) == 0600);

        // a client that goes away without saying goodbye: the server drops its session
        {
            rpc::Client doomed = rpc::Client::connect("unix://" + server.localPath());
            CHECK(server.listener().endpoint().stats().sessions == 2);
        }
        for (int i = 0; i < 100 && server.listener().endpoint().stats().sessions != 1; ++i)
            std::this_thread::sleep_for(20ms);
        CHECK(server.listener().endpoint().stats().sessions == 1);
    }
    {
        // refused by the hook
        rpc::EndpointOptions refuse;
        refuse.authorize([](const rpc::Handshake&) -> rpc::EndpointOptions::AuthorizeResult { return 403; });
        TestServer refusing(std::move(refuse));
        CHECK(throwsCode(rpc::Code::PermissionDenied, [&] { rpc::Client::connect("unix://" + refusing.localPath()); }));
    }
    // nothing there, or not a librpc socket
    CHECK(throwsCode(rpc::Code::Unavailable, [] { rpc::Client::connect("unix:///tmp/librpc-does-not-exist.sock"); }));
}

// A server in its own process, as the IPC is meant to be used.
void testOtherProcess(const char* serverBinary) {
    std::puts("a server in another process");
    const std::string path = "/tmp/librpc-xproc-" + std::to_string(::getpid()) + ".sock";
    const std::string arg = "--local=" + path;
    char* argv[] = {const_cast<char*>(serverBinary), const_cast<char*>("0"), const_cast<char*>(arg.c_str()), nullptr};
    pid_t pid = 0;
    if (::posix_spawn(&pid, serverBinary, nullptr, nullptr, argv, environ) != 0) {
        std::fprintf(stderr, "  cannot start %s\n", serverBinary);
        CHECK(false);
        return;
    }
    rpc::Client client;
    for (int i = 0; i < 100; ++i) {
        try {
            client = rpc::Client::connect("unix://" + path);
            break;
        } catch (const rpc::Error&) {
            std::this_thread::sleep_for(50ms);
        }
    }
    CHECK(client.connected());
    if (client.connected()) {
        CalcClient calc(client);
        CHECK(calc.Add(20, 22) == 42);
        const std::string big(500 * 1024, 'q');
        CHECK(calc.Echo(big)->result() == big);
        int n = 0;
        for (auto s = calc.Count(1, 500, 1, 0, 8 * 1024); s.next();)
            n += s->payload().size() == 8 * 1024;
        CHECK(n == 500);
        // the server process dies: calls fail instead of hanging
        ::kill(pid, SIGKILL);
        int status = 0;
        ::waitpid(pid, &status, 0);
        CHECK(throwsCode(rpc::Code::Unavailable, [&] {
            for (int i = 0; i < 100; ++i) {
                calc.Add(1, 1);
                std::this_thread::sleep_for(20ms);
            }
        }));
    } else {
        ::kill(pid, SIGKILL);
        ::waitpid(pid, nullptr, 0);
    }
    ::unlink(path.c_str());
}

class CountingCalc : public CalcImpl {
public:
    // Returns how many calls this session has made: state kept per connection.
    uint32_t Sleep(rpc::ServerContext& context, uint32_t millis) override {
        auto counter = context.sessionState<int>();
        if (!counter) {
            counter = std::make_shared<int>(0);
            context.setSessionState(counter);
        }
        if (millis)
            std::this_thread::sleep_for(std::chrono::milliseconds(millis));
        return static_cast<uint32_t>(++*counter);
    }
};

void testSessionFeatures() {
    std::puts("per-session state, serial execution, inline handlers");
    for (bool local : {false, true}) {
        g_local = local;
        std::atomic<int> closes{0};
        std::atomic<int> closedValue{0};
        http::ServerConfig config;
        config.host = "127.0.0.1";
        config.port = freePort();
        config.ioThreads = 1;
        const std::string path = "/tmp/librpc-session-" + std::to_string(::getpid()) + (local ? "-l" : "-w") + ".sock";
        CountingCalc service;
        rpc::Listener listener(config, rpc::EndpointOptions()
                                           .serialPerSession()
                                           .inlineUnary()
                                           .onSessionClose([&](uint64_t, std::shared_ptr<void> state) {
                                               closedValue = state ? *std::static_pointer_cast<int>(state) : -1;
                                               ++closes;
                                           }));
        listener.add(service).listenLocal(path);
        std::thread thread([&] { listener.run(); });
        const std::string url = local ? "unix://" + path : "ws://127.0.0.1:" + std::to_string(config.port) + "/rpc";
        rpc::Client client;
        for (int i = 0; i < 100; ++i) {
            try {
                client = rpc::Client::connect(url);
                break;
            } catch (const rpc::Error&) {
                std::this_thread::sleep_for(50ms);
            }
        }
        CalcClient calc(client);
        // state lives with the connection ...
        CHECK(calc.Sleep(0) == 1);
        CHECK(calc.Sleep(0) == 2);
        {
            rpc::Client other = rpc::Client::connect(url);
            CHECK(CalcClient(other).Sleep(0) == 1);
        }
        // ... and calls of one session do not overlap: the quick call waits for the slow one
        const auto start = std::chrono::steady_clock::now();
        std::thread slow([&] { calc.Sleep(300); });
        std::this_thread::sleep_for(50ms);
        CHECK(calc.Add(1, 2) == 3);
        CHECK(std::chrono::steady_clock::now() - start >= 280ms);
        slow.join();
        client.close();
        for (int i = 0; i < 100 && closes < 2; ++i)
            std::this_thread::sleep_for(20ms);
        CHECK(closes >= 2);
        CHECK(closedValue >= 1);
        listener.stop();
        thread.join();
    }
    g_local = false;
}


// ---- shared blobs -------------------------------------------------------------------------------------------------

// Polls `fn` until it is true.
template <class Fn>
bool eventually(Fn&& fn, std::chrono::milliseconds timeout = 3s) {
    const auto until = std::chrono::steady_clock::now() + timeout;
    while (!fn()) {
        if (std::chrono::steady_clock::now() >= until)
            return false;
        std::this_thread::sleep_for(5ms);
    }
    return true;
}

// A pattern of `size` bytes, and the sum a server computes for it.
void fillPattern(uint8_t* data, size_t size, int seed) {
    for (size_t i = 0; i < size; ++i)
        data[i] = static_cast<uint8_t>(i * 7 + seed);
}
std::string expectedLine(size_t size, int seed, bool shared) {
    uint64_t sum = 0;
    for (size_t i = 0; i < size; ++i)
        sum += static_cast<uint8_t>(i * 7 + seed);
    return std::to_string(sum) + ":" + std::to_string(size) + ":" + (shared ? "1" : "0");
}

rpc::EndpointOptions blobEndpoint(uint64_t perSession = 8 << 20) { return rpc::EndpointOptions().sharedArena(perSession); }
rpc::ClientOptions blobClient(uint64_t arena = 4 << 20) {
    rpc::ClientOptions options;
    options.sharedArenaBytes = arena;
    return options;
}

// Over a local socket the blobs travel by reference; over a WebSocket they are copied inline. Same calls, same answers.
void testBlobsRoundTrip() {
    std::puts("shared blobs: unary, client streams and bidirectional calls");
    TestServer server(blobEndpoint());
    rpc::Client client = server.connect(blobClient());
    test::BlobsClient blobs(client);
    const bool byReference = g_local;
    CHECK(client.stats().arenaBytes == (g_local ? (4u << 20) : 0u));

    // unary: a block of the shared area (or its inline fallback), and a plain span
    {
        rpc::SharedBuffer buffer = client.allocateShared(100000);
        CHECK(buffer.size() == 100000 && buffer.shared() == byReference);
        fillPattern(buffer.data(), buffer.size(), 3);
        auto answer = blobs.Scan(std::move(buffer), 640);
        CHECK(answer->result() == expectedLine(100000, 3, byReference) + ":640");
        std::vector<uint8_t> plain(5000);
        fillPattern(plain.data(), plain.size(), 9);
        CHECK(blobs.Scan(rpc::Span<const uint8_t>(plain), 1)->result() == expectedLine(5000, 9, false) + ":1");
        CHECK(blobs.Scan(rpc::Span<const uint8_t>(), 2)->result() == "0:0:0:2");
        // two blobs in one call
        rpc::SharedBuffer a = client.allocateShared(4096), b = client.allocateShared(10);
        fillPattern(a.data(), a.size(), 1);
        fillPattern(b.data(), b.size(), 2);
        CHECK(blobs.Scan2(std::move(a), std::move(b))->result() == expectedLine(4096, 1, byReference) + "," + expectedLine(10, 2, byReference));
    }
    CHECK(client.stats().arenaInUse == 0);
    CHECK(client.stats().blobsShared == (g_local ? 3u : 0u));

    // a client stream whose items carry blobs
    {
        auto up = blobs.Upload();
        std::string expected;
        for (int i = 0; i < 6; ++i) {
            rpc::SharedBuffer buffer = client.allocateShared(8000 + i);
            fillPattern(buffer.data(), buffer.size(), i);
            expected += (i ? ";" : "") + std::to_string(i) + "=" + expectedLine(8000 + i, i, byReference);
            if (buffer.shared()) {
                CHECK(up.write([&](test::Frame::Builder& p) { p.setSequence(i); }, rpc::Share{test::FrameShared::pixels, std::move(buffer)}));
            } else {
                CHECK(up.write([&](test::Frame::Builder& p) { p.setSequence(i); p.setPixels(buffer.data(), buffer.size()); }));
            }
        }
        CHECK(up.finish()->result() == expected);
    }
    // ... and a bidirectional one, read as it goes
    {
        auto echo = blobs.Echo();
        for (int i = 0; i < 5; ++i) {
            rpc::SharedBuffer buffer = client.allocateShared(3000);
            fillPattern(buffer.data(), buffer.size(), 40 + i);
            if (buffer.shared()) {
                CHECK(echo.write([&](test::Frame::Builder& p) { p.setSequence(i); }, rpc::Share{test::FrameShared::pixels, std::move(buffer)}));
            } else {
                CHECK(echo.write([&](test::Frame::Builder& p) { p.setSequence(i); p.setPixels(buffer.data(), buffer.size()); }));
            }
            CHECK(echo.next());
            CHECK(echo->result() == std::to_string(i) + "=" + expectedLine(3000, 40 + i, byReference));
        }
        echo.closeSend();
        CHECK(!echo.next());
    }
    CHECK(eventually([&] { return client.stats().arenaInUse == 0; }));
}

// Falling back to inline fields wherever the shared area is not there.
void testBlobsInline() {
    std::puts("shared blobs: inline fallback");
    const auto scan = [](rpc::Client& client, size_t size, bool expectShared) {
        test::BlobsClient blobs(client);
        rpc::SharedBuffer buffer = client.allocateShared(size);
        CHECK(buffer.shared() == expectShared);
        fillPattern(buffer.data(), buffer.size(), 5);
        CHECK(blobs.Scan(std::move(buffer), 3)->result() == expectedLine(size, 5, expectShared) + ":3");
    };
    const bool before = g_local;
    g_local = true;
    {
        // a client that asks for no area
        TestServer server(blobEndpoint());
        rpc::Client client = server.connect();
        CHECK(client.stats().arenaBytes == 0);
        scan(client, 20000, false);
        // a server that offers none
        TestServer plain;
        rpc::Client other = plain.connect(blobClient());
        CHECK(other.stats().arenaBytes == 0);
        scan(other, 20000, false);
        CHECK(other.stats().blobsInline == 1);
        // an area the server's cap cuts down
        TestServer small(blobEndpoint(1 << 20));  // the cap is announced in MiB
        rpc::Client cut = small.connect(blobClient(4 << 20));
        CHECK(cut.stats().arenaBytes == (1u << 20));
        scan(cut, 20000, true);
    }
    g_local = before;
}

// An area that is full: later blobs are inline, the calls still succeed.
void testBlobsExhaustion() {
    std::puts("shared blobs: a full area");
    const bool before = g_local;
    g_local = true;
    TestServer server(blobEndpoint());
    rpc::Client client = server.connect(blobClient(64 << 10));
    test::BlobsClient blobs(client);
    CHECK(client.stats().arenaBytes == 64u << 10);
    std::vector<rpc::SharedBuffer> held;
    for (int i = 0; i < 5; ++i) {
        held.push_back(client.allocateShared(16 << 10));
        fillPattern(held.back().data(), held.back().size(), i);
    }
    CHECK(held[0].shared() && held[3].shared() && !held[4].shared());
    CHECK(client.stats().blobsInline == 1);
    for (int i = 0; i < 5; ++i) {
        const bool shared = held[i].shared();
        CHECK(blobs.Scan(std::move(held[i]), i)->result() == expectedLine(16 << 10, i, shared) + ":" + std::to_string(i));
    }
    CHECK(client.stats().arenaInUse == 0);
    // the blocks are reused
    rpc::SharedBuffer again = client.allocateShared(16 << 10);
    CHECK(again.shared());
    // one larger than the whole area is inline too
    CHECK(!client.allocateShared(1 << 20).shared());
    g_local = before;
}

// When the blocks come back.
void testBlobsLeases() {
    std::puts("shared blobs: leases");
    const bool before = g_local;
    g_local = true;
    TestServer server(blobEndpoint());
    rpc::Client client = server.connect(blobClient(1 << 20));
    test::BlobsClient blobs(client);

    // unary: the block is the server's until the call ends
    {
        rpc::SharedBuffer buffer = client.allocateShared(64 << 10);
        fillPattern(buffer.data(), buffer.size(), 1);
        std::string answer;
        std::thread caller([&] { answer = std::string(blobs.Slow(std::move(buffer), 400)->result()); });
        std::this_thread::sleep_for(150ms);
        CHECK(client.stats().arenaInUse >= (64u << 10));
        caller.join();
        CHECK(answer == expectedLine(64 << 10, 1, true));
        CHECK(client.stats().arenaInUse == 0);
    }
    // a call that gives up keeps its block until the server's End
    {
        rpc::SharedBuffer buffer = client.allocateShared(64 << 10);
        fillPattern(buffer.data(), buffer.size(), 2);
        CHECK(throwsCode(rpc::Code::DeadlineExceeded, [&] { blobs.Slow(std::move(buffer), 400, rpc::CallOptions{50ms}); }));
        CHECK(client.stats().arenaInUse >= (64u << 10));
        CHECK(eventually([&] { return client.stats().arenaInUse == 0; }));
        // ... and the connection is as good as new
        CHECK(blobs.Scan(rpc::Span<const uint8_t>(), 0)->result() == "0:0:0:0");
    }
    // a stream without take(): the blocks come back as the items are read
    {
        auto up = blobs.Upload();
        for (int i = 0; i < 20; ++i) {
            rpc::SharedBuffer buffer = client.allocateShared(10000);
            fillPattern(buffer.data(), buffer.size(), i);
            CHECK(up.write([&](test::Frame::Builder& p) { p.setSequence(i); }, rpc::Share{test::FrameShared::pixels, std::move(buffer)}));
        }
        CHECK(up.finish()->result().size() > 20);
        CHECK(eventually([&] { return client.stats().arenaInUse == 0; }));
    }
    // a stream with take(): a held item keeps its block, and releasing it (on another thread) gives it back
    {
        auto keep = blobs.Keep();
        for (int i = 0; i < 4; ++i) {
            rpc::SharedBuffer buffer = client.allocateShared(10000);
            fillPattern(buffer.data(), buffer.size(), 10 + i);
            CHECK(keep.write([&](test::Frame::Builder& p) { p.setSequence(i); }, rpc::Share{test::FrameShared::pixels, std::move(buffer)}));
        }
        CHECK(keep.finish()->result() == 4u);
        CHECK(eventually([&] { return server.blobs().describeHeld().size() == 4; }));
        std::this_thread::sleep_for(100ms);
        CHECK(client.stats().arenaInUse > 0);
        const auto held = server.blobs().describeHeld();
        for (int i = 0; i < 4; ++i)
            CHECK(held[i] == expectedLine(10000, 10 + i, true));
        std::thread other([&] { server.blobs().releaseHeld(); });
        other.join();
        CHECK(eventually([&] { return client.stats().arenaInUse == 0; }));
    }
    // a handler that returns early: the items it never read come back
    {
        auto some = blobs.Some(1);
        for (int i = 0; i < 6; ++i) {
            rpc::SharedBuffer buffer = client.allocateShared(10000);
            if (!some.write([&](test::Frame::Builder& p) { p.setSequence(i); }, rpc::Share{test::FrameShared::pixels, std::move(buffer)}))
                break;
        }
        CHECK(some.finish()->result() == 1u);
        CHECK(eventually([&] { return client.stats().arenaInUse == 0; }));
    }
    // a buffer never sent is just freed
    {
        { rpc::SharedBuffer unused = client.allocateShared(100000); CHECK(client.stats().arenaInUse > 0); }
        CHECK(client.stats().arenaInUse == 0);
    }
    g_local = before;
}

// A 25 MB blob through rings a fraction of its size.
void testBlobsLarge() {
    std::puts("shared blobs: far larger than the ring");
    const bool before = g_local;
    g_local = true;
    TestServer server(rpc::EndpointOptions().maxMessage(1 << 20).sharedArena(64 << 20));
    rpc::Client client = server.connect(blobClient(64 << 20));
    test::BlobsClient blobs(client);
    const size_t size = 25 << 20;
    rpc::SharedBuffer buffer = client.allocateShared(size);
    CHECK(buffer.shared());
    fillPattern(buffer.data(), buffer.size(), 11);
    CHECK(blobs.Scan(std::move(buffer), 1)->result() == expectedLine(size, 11, true) + ":1");
    // inline, the same bytes would not fit a message
    rpc::Client inlineClient = server.connect();
    std::vector<uint8_t> big(size, 1);
    CHECK(throwsCode(rpc::Code::ResourceExhausted, [&] { test::BlobsClient(inlineClient).Scan(rpc::Span<const uint8_t>(big), 1); }));
    g_local = before;
}

// ---- a hand-driven local client: says what a real Client never would ---------------------------------------------

class RawLocal {
public:
    explicit RawLocal(const std::string& path) {
        socket_ = bufs::local::Socket::connect(path);
        pollfd p{socket_.fd(), POLLIN, 0};
        if (::poll(&p, 1, 5000) <= 0)
            throw std::runtime_error("no handshake");
        bufs::local::ReceivedFds fds;
        rpc::detail::IpcHello hello;
        if (socket_.receive(&hello, sizeof hello, fds) != static_cast<long>(sizeof hello) || hello.status != 0)
            throw std::runtime_error("bad handshake");
        arenaMiB = hello.arenaMiB;
        channel_ = bufs::shm::Channel::adopt(fds.take(0), 1ull << 32);
    }

    // The shared area message, the way a client sends it (or not).
    void sendArena(uint64_t announced, int fd, uint32_t magic = rpc::detail::IpcArena::kMagic) {
        rpc::detail::IpcArena message;
        message.magic = magic;
        message.bytes = announced;
        socket_.send(&message, sizeof message, &fd, 1);
    }

    void send(const std::pair<const uint8_t*, size_t>& env, const uint8_t* body = nullptr, size_t bodySize = 0) {
        uint8_t* slot = channel_.send().reserve(env.second + bodySize, std::chrono::seconds(5), std::chrono::nanoseconds(0));
        if (!slot)
            throw std::runtime_error("ring full");
        std::memcpy(slot, env.first, env.second);
        if (bodySize)
            std::memcpy(slot + env.second, body, bodySize);
        channel_.send().commit(env.second + bodySize);
    }

    void hello(uint64_t arena) {
        send(rpc::detail::envelope([&](rpc::wire::Envelope::Builder& e) {
            e.setKind(rpc::wire::Kind::Hello);
            e.setProtocol(rpc::detail::kProtocol);
            if (arena)
                e.setArena(arena);
        }));
    }

    struct Ref {
        uint32_t field;
        uint64_t offset;
        uint64_t length;
    };

    // A Scan call (pixels left empty, width 7) with the given blob references.
    void scan(uint32_t id, const std::vector<Ref>& refs, bool streaming = false) {
        bufs::MessageBuilder builder;
        builder.initRoot<test::BlobsScanRequest>().setWidth(7);
        const auto sealed = builder.finish();
        send(rpc::detail::envelope([&](rpc::wire::Envelope::Builder& e) {
                 e.setId(id);
                 e.setKind(rpc::wire::Kind::Call);
                 e.setMethod(test::BlobsMethods::Scan);
                 (void)streaming;
                 fillRefs(e, refs);
             }),
             sealed.first, sealed.second);
    }

    // An Upload call, then one item with the given references.
    void upload(uint32_t id) {
        bufs::MessageBuilder builder;
        builder.initRoot<test::BlobsUploadRequest>();
        const auto sealed = builder.finish();
        send(rpc::detail::envelope([&](rpc::wire::Envelope::Builder& e) {
                 e.setId(id);
                 e.setKind(rpc::wire::Kind::Call);
                 e.setMethod(test::BlobsMethods::Upload);
             }),
             sealed.first, sealed.second);
    }
    void item(uint32_t id, const std::vector<Ref>& refs) {
        bufs::MessageBuilder builder;
        builder.initRoot<test::Frame>().setSequence(1);
        const auto sealed = builder.finish();
        send(rpc::detail::envelope([&](rpc::wire::Envelope::Builder& e) {
                 e.setId(id);
                 e.setKind(rpc::wire::Kind::Data);
                 fillRefs(e, refs);
             }),
             sealed.first, sealed.second);
    }
    void end(uint32_t id) {
        send(rpc::detail::envelope([&](rpc::wire::Envelope::Builder& e) {
            e.setId(id);
            e.setKind(rpc::wire::Kind::End);
        }));
    }

    // The next frame within `timeout`; false if none came.
    bool read(std::chrono::milliseconds timeout = 3s) {
        const auto until = std::chrono::steady_clock::now() + timeout;
        for (;;) {
            const uint8_t* data = nullptr;
            size_t size = 0;
            if (channel_.receive().front(data, size)) {
                frame_ = bufs::BufferPool::global().acquire(size);
                std::memcpy(frame_.data(), data, size);
                channel_.receive().pop();
                if (!rpc::detail::parseFrame(frame_.data(), size, parsed_))
                    throw std::runtime_error("malformed frame from the server");
                return true;
            }
            if (std::chrono::steady_clock::now() >= until)
                return false;
            channel_.receive().wait(10ms, std::chrono::nanoseconds(0));
        }
    }
    const rpc::wire::Envelope::Reader& envelope() const { return parsed_.envelope; }

    // Frames until the End of call `id` (Credit and Release frames are skipped): its code, or -1.
    int endCode(uint32_t id) {
        while (read()) {
            if (envelope().kind() == rpc::wire::Kind::End && envelope().id() == id)
                return envelope().code();
        }
        return -1;
    }
    // The server's answer to the Hello: the shared area it accepted.
    int64_t welcomeArena() {
        if (!read())
            return -1;
        return envelope().kind() == rpc::wire::Kind::Welcome ? static_cast<int64_t>(envelope().arena()) : -1;
    }
    // True once the server has closed the connection.
    bool closedByServer(std::chrono::milliseconds timeout = 3s) {
        return eventually([&] {
            pollfd p{socket_.fd(), POLLRDHUP, 0};
            return ::poll(&p, 1, 0) > 0 && (p.revents & (POLLHUP | POLLERR | POLLRDHUP));
        }, timeout);
    }

    uint32_t arenaMiB = 0;

private:
    static void fillRefs(rpc::wire::Envelope::Builder& e, const std::vector<Ref>& refs) {
        if (refs.empty())
            return;
        auto list = e.initBlobs(refs.size());
        for (size_t i = 0; i < refs.size(); ++i) {
            rpc::wire::BlobRef ref;
            ref.field = refs[i].field;
            ref.reserved = 0;
            ref.offset = refs[i].offset;
            ref.length = refs[i].length;
            list.set(i, ref);
        }
    }

    bufs::local::Socket socket_;
    bufs::shm::Channel channel_;
    bufs::Buffer frame_;
    rpc::detail::Frame parsed_;
};

// A memfd of `bytes` with the seals a client puts on it (or `seals`).
bufs::Fd makeArenaFd(size_t bytes, int seals = F_SEAL_SHRINK | F_SEAL_GROW | F_SEAL_SEAL) {
    bufs::Fd fd(static_cast<int>(::syscall(SYS_memfd_create, "test-arena", MFD_CLOEXEC | MFD_ALLOW_SEALING)));
    if (!fd || ::ftruncate(fd.get(), static_cast<off_t>(bytes)) != 0)
        throw std::runtime_error("memfd");
    if (seals && ::fcntl(fd.get(), F_ADD_SEALS, seals) != 0)
        throw std::runtime_error("seals");
    return fd;
}

// What a hostile or broken client may send.
void testBlobsValidation() {
    std::puts("shared blobs: references are checked");
    const bool before = g_local;
    g_local = true;
    TestServer server(blobEndpoint());
    const uint32_t kPixels = test::BlobsScanRequestShared::pixels;
    const uint64_t kArena = 64 << 10;
    {
        RawLocal c(server.localPath());
        CHECK(c.arenaMiB == 8);
        bufs::Fd fd = makeArenaFd(kArena);
        c.sendArena(kArena, fd.get());
        c.hello(kArena);
        CHECK(c.welcomeArena() == static_cast<int64_t>(kArena));
        const int ok = 0, invalid = static_cast<int>(rpc::Code::InvalidArgument);
        uint32_t id = 1;
        const auto expect = [&](std::vector<RawLocal::Ref> refs, int code) {
            c.scan(id, refs);
            CHECK(c.endCode(id) == code);
            ++id;
        };
        expect({{kPixels, 8, 16}}, ok);                    // a good one
        expect({{kPixels, 0, kArena}}, ok);                // the whole area
        expect({{kPixels, kArena, 0}}, ok);                // empty, at the end
        expect({{kPixels, kArena + 8, 0}}, invalid);       // starts past the end
        expect({{kPixels, 8, kArena}}, invalid);           // runs past the end
        expect({{kPixels, 8, UINT64_MAX}}, invalid);       // offset + size overflows
        expect({{kPixels, UINT64_MAX - 7, 16}}, invalid);  // so does this
        expect({{kPixels, 4, 16}}, invalid);               // unaligned
        expect({{5, 8, 16}}, invalid);                     // not a shared field of the request
        expect({{kPixels, 8, 16}, {kPixels, 32, 16}}, invalid);  // the same field twice
        expect({{kPixels, 8, 16}}, ok);                    // and the connection still works
        // a client stream item with a bad reference ends that call, not the connection
        c.upload(id);
        c.item(id, {{test::FrameShared::pixels, 8, 16}});
        c.item(id, {{test::FrameShared::pixels, kArena, 16}});
        CHECK(c.endCode(id) == invalid);
        ++id;
        expect({{kPixels, 8, 16}}, ok);
        // five references: a protocol error, the connection is closed
        c.scan(id, {{0, 0, 8}, {1, 8, 8}, {2, 16, 8}, {3, 24, 8}, {4, 32, 8}});
        CHECK(c.closedByServer());
    }
    {
        // references without any shared area
        RawLocal c(server.localPath());
        c.hello(0);
        CHECK(c.welcomeArena() == 0);
        c.scan(1, {{kPixels, 0, 8}});
        CHECK(c.closedByServer());
    }
    {
        // ... and over a WebSocket
        http::WebSocketClient ws;
        http::WebSocketClientOptions options;
        options.subprotocols = {rpc::detail::kSubprotocol};
        ws.connect("ws://127.0.0.1:" + std::to_string(server.port()) + "/rpc", options);
        const auto hello = rpc::detail::envelope([&](rpc::wire::Envelope::Builder& e) {
            e.setKind(rpc::wire::Kind::Hello);
            e.setProtocol(rpc::detail::kProtocol);
        });
        ws.sendBinary(hello.first, hello.second);
        CHECK(ws.receive(std::chrono::steady_clock::now() + 3s, 15000ms).has_value());
        bufs::MessageBuilder builder;
        builder.initRoot<test::BlobsScanRequest>().setWidth(7);
        const auto sealed = builder.finish();
        const auto env = rpc::detail::envelope([&](rpc::wire::Envelope::Builder& e) {
            e.setId(1);
            e.setKind(rpc::wire::Kind::Call);
            e.setMethod(test::BlobsMethods::Scan);
            auto list = e.initBlobs(1);
            rpc::wire::BlobRef ref;
            ref.field = kPixels;
            ref.reserved = 0;
            ref.offset = 0;
            ref.length = 8;
            list.set(0, ref);
        });
        std::vector<uint8_t> frame(env.second + sealed.second);
        std::memcpy(frame.data(), env.first, env.second);
        std::memcpy(frame.data() + env.second, sealed.first, sealed.second);
        ws.sendBinary(frame.data(), frame.size());
        bool closed = false;
        try {
            for (int i = 0; i < 10; ++i)
                ws.receive(std::chrono::steady_clock::now() + 1s, 15000ms);
        } catch (const http::WebSocketError& error) {
            closed = error.kind() == http::WebSocketError::Kind::Closed && error.closeCode() == 1002;
        }
        CHECK(closed);
    }
    g_local = before;
}

// An area the server refuses is not an error: the connection goes on with inline fields.
void testBlobsHandshake() {
    std::puts("shared blobs: refused areas");
    const bool before = g_local;
    g_local = true;
    const uint64_t kArena = 64 << 10;
    TestServer server(blobEndpoint(1 << 20));  // the per-session cap is announced in whole MiB
    const auto refused = [&](const char* what, uint64_t announced, auto&& send) {
        RawLocal c(server.localPath());
        send(c);
        c.hello(announced);
        const int64_t accepted = c.welcomeArena();
        if (accepted != 0)
            std::fprintf(stderr, "  (%s: the server accepted %lld)\n", what, static_cast<long long>(accepted));
        CHECK(accepted == 0);
        c.scan(1, {});  // inline: no references
        const int code = c.endCode(1);
        if (code != 0)
            std::fprintf(stderr, "  (%s: the connection did not go on, End %d)\n", what, code);
        CHECK(code == 0);
    };
    refused("unsealed", kArena, [&](RawLocal& c) { bufs::Fd fd = makeArenaFd(kArena, 0); c.sendArena(kArena, fd.get()); });
    refused("only shrink-sealed", kArena, [&](RawLocal& c) { bufs::Fd fd = makeArenaFd(kArena, F_SEAL_SHRINK); c.sendArena(kArena, fd.get()); });
    refused("smaller than announced", kArena, [&](RawLocal& c) { bufs::Fd fd = makeArenaFd(kArena / 2); c.sendArena(kArena, fd.get()); });
    refused("announced differently", kArena, [&](RawLocal& c) { bufs::Fd fd = makeArenaFd(kArena); c.sendArena(kArena / 2, fd.get()); });
    refused("over the cap", 2 << 20, [&](RawLocal& c) { bufs::Fd fd = makeArenaFd(2 << 20); c.sendArena(2 << 20, fd.get()); });
    refused("never sent", kArena, [&](RawLocal&) {});
    refused("wrong magic", kArena, [&](RawLocal& c) { bufs::Fd fd = makeArenaFd(kArena); c.sendArena(kArena, fd.get(), 0x12345678); });
    refused("not a page multiple", 1000, [&](RawLocal& c) { bufs::Fd fd = makeArenaFd(1000); c.sendArena(1000, fd.get()); });
    CHECK(server.listener().endpoint().stats().arenas == 0);

    // the total cap over all sessions
    {
        TestServer capped(rpc::EndpointOptions().sharedArena(1 << 20).sharedArenaTotal(100 << 10));
        rpc::Client first = capped.connect(blobClient(kArena));
        rpc::Client second = capped.connect(blobClient(kArena));
        CHECK(first.stats().arenaBytes == kArena && second.stats().arenaBytes == 0);
        CHECK(capped.listener().endpoint().stats().arenas == 1 && capped.listener().endpoint().stats().arenaBytes == kArena);
        // the second still works, inline
        CHECK(test::BlobsClient(second).Scan(rpc::Span<const uint8_t>(), 4)->result() == "0:0:0:4");
        first.close();
        CHECK(eventually([&] { return capped.listener().endpoint().stats().arenas == 0; }));
        rpc::Client third = capped.connect(blobClient(kArena));
        CHECK(third.stats().arenaBytes == kArena);
    }
    g_local = before;
}

struct DeviceStub {
    std::atomic<int> registered{0}, unregistered{0};
    std::atomic<const void*> base{nullptr};
    std::atomic<size_t> bytes{0};
};

// The device sees the area once, and it stays registered while anything still reads it.
void testBlobsRegistration() {
    std::puts("shared blobs: device registration");
    const bool before = g_local;
    g_local = true;
    DeviceStub stub;
    rpc::DeviceRegistration registration;
    registration.ctx = &stub;
    registration.registerMemory = [](void* ctx, void* data, size_t bytes) -> void* {
        auto* s = static_cast<DeviceStub*>(ctx);
        ++s->registered;
        s->base = data;
        s->bytes = bytes;
        return static_cast<uint8_t*>(data) + 1;  // a fake device address, to be told from the host's
    };
    registration.unregisterMemory = [](void* ctx, void* data, size_t bytes) {
        auto* s = static_cast<DeviceStub*>(ctx);
        if (data == s->base && bytes == s->bytes)
            ++s->unregistered;
    };
    TestServer server(blobEndpoint().sharedRegistration(registration));
    {
        rpc::Client client = server.connect(blobClient(1 << 20));
        test::BlobsClient blobs(client);
        CHECK(stub.registered == 1 && stub.unregistered == 0 && stub.bytes == (1u << 20));
        auto keep = blobs.Keep();
        for (int i = 0; i < 3; ++i) {
            rpc::SharedBuffer buffer = client.allocateShared(5000);
            fillPattern(buffer.data(), buffer.size(), i);
            CHECK(keep.write([&](test::Frame::Builder& p) { p.setSequence(i); }, rpc::Share{test::FrameShared::pixels, std::move(buffer)}));
        }
        CHECK(keep.finish()->result() == 3u);
        CHECK(eventually([&] { return server.blobs().heldPointers().size() == 3; }));
        for (const auto& pointers : server.blobs().heldPointers()) {
            // device = (host address in the mapping) + 1, and the host address lies in the registered area
            CHECK(pointers.second == static_cast<const uint8_t*>(pointers.first) + 1);
            CHECK(pointers.first >= stub.base && static_cast<const uint8_t*>(pointers.first) < static_cast<const uint8_t*>(stub.base.load()) + stub.bytes);
        }
    }
    // the client is gone, its session ends, but the held items still pin the registration
    CHECK(eventually([&] { return server.listener().endpoint().stats().sessions == 0; }));
    std::this_thread::sleep_for(100ms);
    CHECK(stub.registered == 1 && stub.unregistered == 0);
    CHECK(server.listener().endpoint().stats().arenas == 1);
    CHECK(server.blobs().describeHeld()[1] == expectedLine(5000, 1, true));
    server.blobs().releaseHeld();
    CHECK(eventually([&] { return stub.unregistered == 1; }));
    CHECK(server.listener().endpoint().stats().arenas == 0 && server.listener().endpoint().stats().arenaBytes == 0);
    // a device that refuses: the connection goes on without a shared area
    {
        rpc::DeviceRegistration refuse = registration;
        refuse.registerMemory = [](void*, void*, size_t) -> void* { return nullptr; };
        TestServer refusing(blobEndpoint().sharedRegistration(refuse));
        rpc::Client client = refusing.connect(blobClient(1 << 20));
        CHECK(client.stats().arenaBytes == 0);
        CHECK(test::BlobsClient(client).Scan(rpc::Span<const uint8_t>(), 4)->result() == "0:0:0:4");
    }
    g_local = before;
}

// A client process that is killed while the server holds one of its items.
void testBlobsDeadClient(const char* self) {
    std::puts("shared blobs: a client that dies");
    const bool before = g_local;
    g_local = true;
    DeviceStub stub;
    rpc::DeviceRegistration registration;
    registration.ctx = &stub;
    registration.registerMemory = [](void* ctx, void* data, size_t bytes) -> void* {
        auto* s = static_cast<DeviceStub*>(ctx);
        ++s->registered;
        s->base = data;
        s->bytes = bytes;
        return data;
    };
    registration.unregisterMemory = [](void* ctx, void*, size_t) { ++static_cast<DeviceStub*>(ctx)->unregistered; };
    TestServer server(blobEndpoint().sharedRegistration(registration));
    const std::string path = server.localPath();
    pid_t pid = 0;
    const char* argv[] = {self, "--blob-child", path.c_str(), nullptr};
    CHECK(::posix_spawn(&pid, self, nullptr, nullptr, const_cast<char* const*>(argv), environ) == 0);
    CHECK(eventually([&] { return server.blobs().heldPointers().size() == 3; }, 10s));
    ::kill(pid, SIGKILL);
    int status = 0;
    ::waitpid(pid, &status, 0);
    CHECK(eventually([&] { return server.listener().endpoint().stats().sessions == 0; }));
    // the bytes are still there to read, and still registered
    const auto held = server.blobs().describeHeld();
    CHECK(held.size() == 3);
    for (size_t i = 0; i < held.size(); ++i)
        CHECK(held[i] == expectedLine(7000, 20 + static_cast<int>(i), true));
    CHECK(stub.registered == 1 && stub.unregistered == 0);
    server.blobs().releaseHeld();
    CHECK(eventually([&] { return stub.unregistered == 1; }));
    g_local = before;
}

// The other end of testBlobsDeadClient: sends three items to Keep and waits to be killed.
int blobChild(const char* path) {
    rpc::ClientOptions options;
    options.sharedArenaBytes = 1 << 20;
    rpc::Client client = rpc::Client::connect(std::string("unix://") + path, options);
    test::BlobsClient blobs(client);
    auto keep = blobs.Keep();
    for (int i = 0; i < 3; ++i) {
        rpc::SharedBuffer buffer = client.allocateShared(7000);
        fillPattern(buffer.data(), buffer.size(), 20 + i);
        if (!keep.write([&](test::Frame::Builder& p) { p.setSequence(i); }, rpc::Share{test::FrameShared::pixels, std::move(buffer)}))
            return 2;
    }
    for (;;)
        std::this_thread::sleep_for(1s);
}

} // namespace

void runAll() {
    testUnary();
    testBadRequests();
    testStreaming();
    testStreamsFromTheClient();
    testInboundMemory();
    testDeadlinesAndCancel();
    testConcurrency();
    testAuth();
    testHandlerErrors();
    testKitchen();
    testNatural();
    testStall();
    testShutdown();
    testBlobsRoundTrip();
}

int main(int argc, char** argv) {
    if (argc > 2 && std::strcmp(argv[1], "--blob-child") == 0)
        return blobChild(argv[2]);
    for (const bool local : {false, true}) {
        g_local = local;
        std::printf("== over %s ==\n", local ? "a local socket" : "a WebSocket");
        runAll();
    }
    g_local = false;
    testLocalSocket();
    testSessionFeatures();
    testBlobsInline();
    testBlobsExhaustion();
    testBlobsLeases();
    testBlobsLarge();
    testBlobsValidation();
    testBlobsHandshake();
    testBlobsRegistration();
    testBlobsDeadClient(argv[0]);
    if (argc > 1)
        testOtherProcess(argv[1]);
    std::printf("%d checks, %d failed\n", g_checks, g_failures);
    return g_failures ? 1 : 0;
}

