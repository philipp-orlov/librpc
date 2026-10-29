// A warm server and client allocate nothing from the heap per call: the messages come from libbufs's pools, the
// frames from libhttp's, the call objects and call slots are recycled. Counted with a replaced operator new, on
// every thread of the process (server and client run here side by side).
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <new>
#include <thread>
#include <unistd.h>

#include "calc_service.hpp"

namespace {
std::atomic<bool> g_counting{false};
std::atomic<long> g_allocations{0};
}  // namespace

void* operator new(size_t size) {
    if (g_counting.load(std::memory_order_relaxed))
        g_allocations.fetch_add(1, std::memory_order_relaxed);
    if (void* p = std::malloc(size ? size : 1))
        return p;
    throw std::bad_alloc();
}
void* operator new[](size_t size) { return operator new(size); }
void operator delete(void* p) noexcept { std::free(p); }
void operator delete[](void* p) noexcept { std::free(p); }
void operator delete(void* p, size_t) noexcept { std::free(p); }
void operator delete[](void* p, size_t) noexcept { std::free(p); }

using namespace sample::calc;
using namespace std::chrono_literals;

namespace {

int g_failures = 0;

template <class Fn>
long allocationsDuring(Fn&& fn) {
    g_allocations = 0;
    g_counting = true;
    fn();
    g_counting = false;
    return g_allocations.load();
}

// `allowed`: a handful over the whole run is the one-off growth of a recycled container to its high-water mark; a per-call
// allocation would show as thousands.
long residentKiB() {
    long pages = 0, resident = 0;
    if (std::FILE* f = std::fopen("/proc/self/statm", "r")) {
        if (std::fscanf(f, "%ld %ld", &pages, &resident) != 2)
            resident = 0;
        std::fclose(f);
    }
    return resident * (sysconf(_SC_PAGESIZE) / 1024);
}

// Memory must not grow with the number of calls either: pools are reused, nothing is retained per call.
template <class Fn>
void flat(const char* what, int calls, Fn&& fn) {
    fn(calls / 4);  // warm up
    const long before = residentKiB();
    fn(calls);
    const long after = residentKiB();
    const bool ok = after - before < 1024;
    std::printf("  %-44s RSS %ld -> %ld KiB over %d calls  %s\n", what, before, after, calls, ok ? "ok" : "GROWS");
    if (!ok)
        ++g_failures;
}

void report(const char* what, long allocations, int calls, long allowed) {
    const bool ok = allocations <= allowed;
    std::printf("  %-44s %6ld allocations in %d calls  %s\n", what, allocations, calls, ok ? "ok" : "TOO MANY");
    if (!ok)
        ++g_failures;
}

void measure(const char* label, const std::string& url) {
    std::printf("%s\n", label);
    rpc::Client client = rpc::Client::connect(url);
    CalcClient calc(client);
    const std::string text(2000, 'x');
    const int n = 20000;
    // warm-up: pools, slots, thread-local builders
    for (int i = 0; i < 2000; ++i) {
        calc.Add(i, 1);
        calc.Echo(text);
        calc.Sleep(0);
        for (auto s = calc.Count(1, 20, 1, 0, 300); s.next();) {
        }
    }
    flat("Add", 100000, [&](int n) { for (int i = 0; i < n; ++i) calc.Add(i, 1); });
    flat("Echo 2 KB", 50000, [&](int n) { for (int i = 0; i < n; ++i) calc.Echo(text); });
    flat("Sleep(0) on a worker", 50000, [&](int n) { for (int i = 0; i < n; ++i) calc.Sleep(0); });
    flat("Count x 20 items", 5000, [&](int n) { for (int i = 0; i < n; ++i) for (auto s = calc.Count(1, 20, 1, 0, 300); s.next();) {} });
    report("Add (unary, scalars)", allocationsDuring([&] { for (int i = 0; i < n; ++i) calc.Add(i, 1); }), n, 8);
    report("Echo 2 KB (string in, string out)", allocationsDuring([&] { for (int i = 0; i < n; ++i) calc.Echo(text); }), n, 8);
    report("Sleep(0) (handler on a worker thread)", allocationsDuring([&] { for (int i = 0; i < n; ++i) calc.Sleep(0); }), n, 8);
    auto upload = [&](int n) {
        for (int i = 0; i < n; ++i) {
            auto total = calc.Total();
            for (int k = 0; k < 20; ++k)
                total.write([&](CalcTotalItem::Builder& b) { b.setItem(k); });
            total.finish();
        }
    };
    auto duplex = [&](int n) {
        for (int i = 0; i < n; ++i) {
            auto run = calc.Running();
            for (int k = 0; k < 20; ++k) {
                run.write([&](CalcRunningItem::Builder& b) { b.setItem(k); });
                run.next();
            }
            run.closeSend();
            while (run.next()) {
            }
        }
    };
    upload(500);
    duplex(500);
    flat("Total (client stream of 20)", 3000, upload);
    flat("Running (20 round trips on one call)", 3000, duplex);
    report("Total: client stream of 20 items", allocationsDuring([&] { upload(n / 20); }), n / 20, 8);
    report("Running: bidirectional, 20 round trips", allocationsDuring([&] { duplex(n / 20); }), n / 20, 8);
    report("Count x 20 items of 300 B", allocationsDuring([&] {
               for (int i = 0; i < n / 20; ++i)
                   for (auto s = calc.Count(1, 20, 1, 0, 300); s.next();) {
                   }
           }),
           n / 20, 0);
}

} // namespace

int main() {
    const std::string path = "/tmp/librpc-alloc-" + std::to_string(::getpid()) + ".sock";
    http::ServerConfig config;
    config.host = "127.0.0.1";
    config.port = 18950;
    config.ioThreads = 1;
    CalcImpl calc;
    rpc::Listener listener(config, rpc::EndpointOptions().workers(2));
    listener.add(calc).listenLocal(path);
    std::thread server([&] { listener.run(); });
    for (int i = 0; i < 100; ++i) {
        try {
            rpc::Client::connect("unix://" + path);
            break;
        } catch (const rpc::Error&) {
            std::this_thread::sleep_for(50ms);
        }
    }
    measure("local socket", "unix://" + path);
    measure("WebSocket", "ws://127.0.0.1:18950/rpc");
    listener.stop();
    server.join();
    std::printf("%s\n", g_failures ? "FAILED" : "ok");
    return g_failures ? 1 : 0;
}
