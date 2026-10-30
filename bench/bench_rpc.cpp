// Round-trip latency and throughput over a WebSocket and over a local socket, against a server in another process.
//   bench_rpc                 runs everything (it starts itself as the server)
//   bench_rpc --serve PORT PATH [--inline]
#include <signal.h>
#include <spawn.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#include "calc_service.hpp"

extern char** environ;
using namespace sample::calc;
using Clock = std::chrono::steady_clock;

namespace {

volatile std::sig_atomic_t g_stop = 0;
void onSignal(int) { g_stop = 1; }

int serve(int port, const char* path, bool inlineUnary) {
    http::ServerConfig config;
    config.host = "127.0.0.1";
    config.port = static_cast<uint16_t>(port);
    config.ioThreads = 1;
    CalcImpl calc;
    rpc::EndpointOptions options;
    options.inlineUnary(inlineUnary).window(1 << 20).workers(4);
    rpc::Listener listener(config, std::move(options));
    listener.add(calc).listenLocal(path);
    std::signal(SIGTERM, onSignal);
    std::signal(SIGINT, onSignal);
    std::printf("ready\n");
    std::fflush(stdout);
    listener.run([] { return g_stop != 0; });
    return 0;
}

struct Server {
    pid_t pid = 0;
    std::string path;
    int port = 0;
    Server(const char* self, int port_, bool inlineUnary) : path("/tmp/librpc-bench-" + std::to_string(::getpid()) + ".sock"), port(port_) {
        const std::string p = std::to_string(port);
        std::vector<char*> argv{const_cast<char*>(self), const_cast<char*>("--serve"), const_cast<char*>(p.c_str()),
                                const_cast<char*>(path.c_str())};
        if (inlineUnary)
            argv.push_back(const_cast<char*>("--inline"));
        argv.push_back(nullptr);
        posix_spawn(&pid, self, nullptr, nullptr, argv.data(), environ);
        for (int i = 0; i < 100; ++i) {  // until it accepts
            try {
                rpc::Client::connect("unix://" + path);
                return;
            } catch (const rpc::Error&) {
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
            }
        }
    }
    ~Server() {
        ::kill(pid, SIGTERM);
        ::waitpid(pid, nullptr, 0);
    }
    std::string ws() const { return "ws://127.0.0.1:" + std::to_string(port) + "/rpc"; }
    std::string ipc() const { return "unix://" + path; }
};

double micros(Clock::duration d) { return std::chrono::duration<double, std::micro>(d).count(); }

void latency(const char* label, CalcClient& calc, int n) {
    std::vector<double> t(n);
    for (int i = 0; i < 2000; ++i)
        calc.Add(1, 2);
    for (int i = 0; i < n; ++i) {
        const auto start = Clock::now();
        volatile double r = calc.Add(i, 2);
        (void)r;
        t[i] = micros(Clock::now() - start);
    }
    std::sort(t.begin(), t.end());
    double sum = 0;
    for (double v : t)
        sum += v;
    std::printf("  %-34s p50 %7.1f us   p99 %7.1f us   mean %7.1f us   %9.0f calls/s\n", label, t[n / 2], t[n * 99 / 100], sum / n,
                1e6 / (sum / n));
}

void echo(const char* label, CalcClient& calc, size_t bytes, int n) {
    const std::string text(bytes, 'x');
    for (int i = 0; i < 20; ++i)
        calc.Echo(text);
    const auto start = Clock::now();
    for (int i = 0; i < n; ++i) {
        auto r = calc.Echo(text);
        if (r->result().size() != bytes)
            std::abort();
    }
    const double seconds = std::chrono::duration<double>(Clock::now() - start).count();
    std::printf("  %-34s %8.1f us/call   %8.0f MB/s each way\n", label, seconds * 1e6 / n, 2.0 * bytes * n / seconds / 1e6 / 2);
}

void parallel(const char* label, rpc::Client& client, int threads, int perThread) {
    std::atomic<int> ready{0};
    std::vector<std::thread> workers;
    const auto start = Clock::now();
    for (int t = 0; t < threads; ++t)
        workers.emplace_back([&] {
            CalcClient calc(client);
            for (int i = 0; i < perThread; ++i)
                calc.Add(i, 1);
        });
    for (auto& w : workers)
        w.join();
    const double seconds = std::chrono::duration<double>(Clock::now() - start).count();
    std::printf("  %-34s %9.0f calls/s\n", label, threads * perThread / seconds);
}

void stream(const char* label, CalcClient& calc, uint32_t payload, int items) {
    const auto start = Clock::now();
    int n = 0;
    for (auto s = calc.Count(1, items, 1, 0, payload); s.next();)
        n += s->payload().size() == payload;
    const double seconds = std::chrono::duration<double>(Clock::now() - start).count();
    std::printf("  %-34s %9.0f items/s   %8.0f MB/s\n", label, n / seconds, static_cast<double>(n) * payload / seconds / 1e6);
}

void run(const char* self, const char* title, bool inlineUnary) {
    std::printf("\n%s\n", title);
    Server server(self, 18900 + (inlineUnary ? 1 : 0), inlineUnary);
    rpc::ClientOptions spin0;
    spin0.ipcSpin = std::chrono::microseconds(0);
    rpc::ClientOptions spin50;
    spin50.ipcSpin = std::chrono::microseconds(50);
    {
        rpc::Client ws = rpc::Client::connect(server.ws());
        CalcClient calc(ws);
        latency("WebSocket, Add(a, b)", calc, 20000);
    }
    for (auto [label, options] : {std::pair<const char*, rpc::ClientOptions>{"local, client spin 0", spin0}, {"local, client spin 20 us", rpc::ClientOptions{}}, {"local, client spin 50 us", spin50}}) {
        rpc::Client c = rpc::Client::connect(server.ipc(), options);
        CalcClient calc(c);
        latency(label, calc, 50000);
    }
    rpc::Client ws = rpc::Client::connect(server.ws());
    rpc::Client ipc = rpc::Client::connect(server.ipc());
    CalcClient wsCalc(ws), ipcCalc(ipc);
    for (size_t bytes : {size_t{4096}, size_t{64} << 10, size_t{512} << 10}) {
        const int n = bytes > (64 << 10) ? 600 : 3000;
        echo(("WebSocket, Echo " + std::to_string(bytes >> 10) + " KiB").c_str(), wsCalc, bytes, n);
        echo(("local,     Echo " + std::to_string(bytes >> 10) + " KiB").c_str(), ipcCalc, bytes, n);
    }
    parallel("WebSocket, 8 threads, one connection", ws, 8, 4000);
    parallel("local,     8 threads, one connection", ipc, 8, 4000);
    stream("WebSocket, stream 16 KiB items", wsCalc, 16 << 10, 8000);
    stream("local,     stream 16 KiB items", ipcCalc, 16 << 10, 8000);
    stream("WebSocket, stream 100 B items", wsCalc, 100, 50000);
    stream("local,     stream 100 B items", ipcCalc, 100, 50000);
}

} // namespace

int main(int argc, char** argv) {
    if (argc >= 4 && !std::strcmp(argv[1], "--serve"))
        return serve(std::atoi(argv[2]), argv[3], argc > 4 && !std::strcmp(argv[4], "--inline"));
    run(argv[0], "Server handlers on a worker thread (default)", false);
    run(argv[0], "Server runs unary handlers on the connection's reader thread (inlineUnary)", true);
    return 0;
}
