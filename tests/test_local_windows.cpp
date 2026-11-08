// The local transport on Windows (unix:// URLs over an AF_UNIX stream socket, src/stream_socket.hpp), through the Calc example:
// calls, streams of both kinds, flow control, many threads, the peer's identity, refusal, a stale socket file, and what a
// shut-down server leaves. test_rpc.cpp covers the same API on the platforms with shared-memory rings; it needs POSIX.
#include "sys/net.hpp"  // winsock2.h first: <windows.h> alone brings in the old winsock.h
#include <windows.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <thread>
#include <vector>

#include "calc_service.hpp"
#include "stream_socket.hpp"

using namespace std::chrono_literals;
using namespace sample::calc;

namespace {

int g_failures = 0;
int g_checks = 0;

#define CHECK(cond)                                                                        \
    do {                                                                                   \
        ++g_checks;                                                                        \
        if (!(cond)) {                                                                     \
            ++g_failures;                                                                  \
            std::fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__, __LINE__, #cond); \
        }                                                                                  \
    } while (0)

template <class Fn>
bool throwsCode(rpc::Code code, Fn&& fn) {
    try {
        fn();
    } catch (const rpc::Error& error) {
        return error.code() == code;
    } catch (...) {
    }
    return false;
}

template <class Fn>
bool throwsAnything(Fn&& fn) {
    try {
        fn();
    } catch (...) {
        return true;
    }
    return false;
}

std::string socketPath(const char* name) {
    return (std::filesystem::temp_directory_path() / ("librpc-test-" + std::to_string(::GetCurrentProcessId()) + "-" + name + ".sock"))
        .string();
}

void testCalls() {
    const std::string path = socketPath("calls");
    CalcImpl calc;
    rpc::Endpoint endpoint;
    endpoint.add(calc).listenLocal(path);
    {
        rpc::Client connection = rpc::Client::connect("unix://" + path);
        CalcClient client(connection);
        CHECK(client.Add(40, 2) == 42);
        CHECK(client.Sum(std::vector<double>{1.5, 2.5, 3.0, 4.0}) == 11);
        auto echo = client.Echo("hello, local socket");
        CHECK(echo->result() == "hello, local socket");

        // server streaming, with payloads, so that the flow-control window has to be refilled along the way
        int count = 0;
        for (auto ticks = client.Count(1, 300, 1, 0, 2000); ticks.next(); ++count)
            CHECK(ticks->n() == count + 1);
        CHECK(count == 300);

        // client streaming
        auto total = client.Total();
        for (int i = 1; i <= 1000; ++i)
            total.write([&](CalcTotalItem::Builder& item) { item.setItem(i); });
        CHECK(total.finish()->result() == 500500);

        // bidirectional: each answer follows its question
        auto running = client.Running();
        double expected = 0;
        for (double v : {1.0, 2.0, 3.0, 4.0}) {
            expected += v;
            running.write([&](CalcRunningItem::Builder& item) { item.setItem(v); });
            CHECK(running.next());
            CHECK(running->result() == expected);
        }
        running.closeSend();

        // 4 MiB up in 64 KiB chunks: well past the window, in one direction
        const std::vector<uint8_t> chunk(64 * 1024, 7);
        auto upload = client.Upload("blob");
        for (int i = 0; i < 64; ++i)
            upload.write([&](Chunk::Builder& c) { c.setData(chunk.data(), chunk.size()); });
        CHECK(upload.finish()->result() == 64u * chunk.size());

        // dividing by zero is the service's error, and the connection goes on
        CHECK(throwsCode(rpc::Code::InvalidArgument, [&] { client.Div(1, 0); }));
        CHECK(client.Add(1, 1) == 2);
    }
}

void testConcurrency() {
    const std::string path = socketPath("threads");
    CalcImpl calc;
    rpc::Endpoint endpoint;
    endpoint.add(calc).listenLocal(path);
    std::atomic<int> wrong{0};
    std::vector<std::thread> threads;
    for (int t = 0; t < 8; ++t)
        threads.emplace_back([&, t] {
            try {
                rpc::Client connection = rpc::Client::connect("unix://" + path);
                CalcClient client(connection);
                for (int i = 0; i < 200; ++i)
                    if (client.Add(t, i) != t + i)
                        ++wrong;
            } catch (...) {
                ++wrong;
            }
        });
    for (auto& thread : threads)
        thread.join();
    CHECK(wrong.load() == 0);

    // One connection, several threads calling.
    rpc::Client shared = rpc::Client::connect("unix://" + path);
    std::atomic<int> sharedWrong{0};
    threads.clear();
    for (int t = 0; t < 4; ++t)
        threads.emplace_back([&, t] {
            CalcClient client(shared);
            for (int i = 0; i < 200; ++i)
                if (client.Add(t, i) != t + i)
                    ++sharedWrong;
        });
    for (auto& thread : threads)
        thread.join();
    CHECK(sharedWrong.load() == 0);
}

void testIdentityAndRefusal() {
    {
        const std::string path = socketPath("identity");
        CalcImpl calc;
        bool local = false;
        int pid = -1, uid = -1;
        std::string address;
        rpc::EndpointOptions options;
        options.authorize([&](const rpc::Handshake& handshake) -> rpc::EndpointOptions::AuthorizeResult {
            local = handshake.local;
            pid = handshake.pid;
            uid = handshake.uid;
            address = std::string(handshake.remoteAddress);
            return rpc::Principal{"me", nullptr};
        });
        rpc::Endpoint endpoint(std::move(options));
        endpoint.add(calc).listenLocal(path);
        rpc::Client connection = rpc::Client::connect("unix://" + path);
        CHECK(local);
        CHECK(pid == static_cast<int>(::GetCurrentProcessId()));
        CHECK(uid == 0);  // this process is the server's own account: "the owner"
        CHECK(address.rfind("pid:", 0) == 0);
        CHECK(CalcClient(connection).Add(1, 2) == 3);
    }
    {
        const std::string path = socketPath("refused");
        CalcImpl calc;
        rpc::EndpointOptions refuse;
        refuse.authorize([](const rpc::Handshake&) -> rpc::EndpointOptions::AuthorizeResult { return 403; });
        rpc::Endpoint endpoint(std::move(refuse));
        endpoint.add(calc).listenLocal(path);
        CHECK(throwsCode(rpc::Code::PermissionDenied, [&] { rpc::Client::connect("unix://" + path); }));
    }
    CHECK(throwsCode(rpc::Code::Unavailable, [] { rpc::Client::connect("unix://" + socketPath("nobody-listens")); }));
}

void testSocketFile() {
    const std::string path = socketPath("file");
    CalcImpl calc;
    {
        // What a process that died left behind is replaced.
        std::ofstream(path) << "left over";
        rpc::Endpoint endpoint;
        endpoint.add(calc).listenLocal(path);
        CHECK(CalcClient(rpc::Client::connect("unix://" + path)).Add(2, 2) == 4);

        // A live listener is not.
        CalcImpl other;
        rpc::Endpoint second;
        CHECK(throwsAnything([&] { second.add(other).listenLocal(path); }));
        // ...and the first one is unharmed by the attempt.
        CHECK(CalcClient(rpc::Client::connect("unix://" + path)).Add(3, 3) == 6);

        endpoint.shutdown(100ms);
        CHECK(!std::filesystem::exists(path));  // shutting down takes the socket file with it
    }
}

void testServerGoesAway() {
    const std::string path = socketPath("away");
    CalcImpl calc;
    auto endpoint = std::make_unique<rpc::Endpoint>();
    endpoint->add(calc).listenLocal(path);
    rpc::Client connection = rpc::Client::connect("unix://" + path);
    CalcClient client(connection);
    CHECK(client.Add(1, 1) == 2);
    endpoint->shutdown(200ms);
    endpoint.reset();
    // The call fails, with an error and not a hang.
    CHECK(throwsAnything([&] { client.Add(1, 1); }));
}

void testClientVanishes() {
    const std::string path = socketPath("vanish");
    CalcImpl calc;
    rpc::Endpoint endpoint;
    endpoint.add(calc).listenLocal(path);
    {
        rpc::Client first = rpc::Client::connect("unix://" + path);
        {
            rpc::Client doomed = rpc::Client::connect("unix://" + path);
            CHECK(endpoint.stats().sessions == 2);
        }
        for (int i = 0; i < 100 && endpoint.stats().sessions != 1; ++i)
            std::this_thread::sleep_for(20ms);
        CHECK(endpoint.stats().sessions == 1);
    }
    for (int i = 0; i < 100 && endpoint.stats().sessions != 0; ++i)
        std::this_thread::sleep_for(20ms);
    CHECK(endpoint.stats().sessions == 0);
}

void testTooLarge() {
    const std::string path = socketPath("large");
    CalcImpl calc;
    rpc::Endpoint endpoint;  // the default limit of a message is 1 MiB
    endpoint.add(calc).listenLocal(path);
    rpc::Client connection = rpc::Client::connect("unix://" + path);
    CalcClient client(connection);
    const std::string big(3 << 20, 'x');
    CHECK(throwsAnything([&] { client.Echo(big); }));
}

void testNotALibrpcSocket() {
    // Something that listens on a Unix socket and says nothing sensible: the client must not hang or crash.
    const std::string path = socketPath("garbage");
    rpc::detail::StreamSocket listener = rpc::detail::StreamSocket::listen(path);
    std::atomic<bool> stop{false};
    std::thread server([&] {
        while (!stop) {
            rpc::detail::StreamSocket peer = listener.accept();
            if (peer) {
                const char noise[] = "this is not a handshake at all, just text of about the length of one";
                peer.sendAll(noise, sizeof noise);
                std::this_thread::sleep_for(100ms);
            } else {
                std::this_thread::sleep_for(10ms);
            }
        }
    });
    CHECK(throwsCode(rpc::Code::Unavailable, [&] { rpc::Client::connect("unix://" + path); }));
    stop = true;
    server.join();
    rpc::detail::removeSocketFile(path);
}

} // namespace

int main() {
    testCalls();
    testConcurrency();
    testIdentityAndRefusal();
    testSocketFile();
    testServerGoesAway();
    testClientVanishes();
    testTooLarge();
    testNotALibrpcSocket();
    std::printf("%d checks, %d failed\n", g_checks, g_failures);
    return g_failures ? 1 : 0;
}
