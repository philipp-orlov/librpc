// A complete server: the Calc service of calc.bufs on ws://0.0.0.0:<port>/rpc.
//
//   calc_server [--host H] [--port N] [--token SECRET] [--window BYTES] [--web DIR]
//
// With --token, clients must send that token (ClientOptions::token); otherwise anyone may call.
// With --web, the files of DIR (see ts/build.mjs) are served at / next to the RPC route.
#include <chrono>
#include <cmath>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <thread>

#include "calc_service.hpp"

namespace {

volatile std::sig_atomic_t g_stop = 0;
void onSignal(int) { g_stop = 1; }

using sample::calc::CalcImpl;

} // namespace

int main(int argc, char** argv) {
    http::ServerConfig config;
    config.host = "127.0.0.1";
    config.port = 8080;
    config.ioThreads = 2;
    std::string token;
    std::string web;
    rpc::EndpointOptions options;
    for (int i = 1; i < argc; ++i) {
        if (!std::strcmp(argv[i], "--port") && i + 1 < argc)
            config.port = static_cast<uint16_t>(std::atoi(argv[++i]));
        else if (!std::strcmp(argv[i], "--host") && i + 1 < argc)
            config.host = argv[++i];
        else if (!std::strcmp(argv[i], "--token") && i + 1 < argc)
            token = argv[++i];
        else if (!std::strcmp(argv[i], "--web") && i + 1 < argc)
            web = argv[++i];
        else if (!std::strcmp(argv[i], "--window") && i + 1 < argc)
            options.window(static_cast<size_t>(std::atoll(argv[++i])));
        else {
            std::fprintf(stderr, "usage: calc_server [--host H] [--port N] [--token SECRET] [--window BYTES] [--web DIR]\n");
            return 2;
        }
    }
    if (!token.empty()) {
        options.requireAuth().authenticate([token](std::string_view presented) -> std::optional<rpc::Principal> {
            if (presented == token)
                return rpc::Principal{"calc-client", nullptr};
            return std::nullopt;
        });
    }
    options.onCall([](const rpc::CallInfo& call) {
        std::printf("%-28s %-18s %8llu us  %s\n", std::string(call.method).c_str(), call.principal.name.c_str(),
                    static_cast<unsigned long long>(call.elapsedMicros), call.status.toString().c_str());
        std::fflush(stdout);
    });

    CalcImpl calc;
    rpc::Listener listener(config, std::move(options));
    listener.add(calc);
    if (!web.empty())
        listener.http().onStaticFiles("/", web);

    std::signal(SIGINT, onSignal);
    std::signal(SIGTERM, onSignal);
    std::printf("calc_server: ws://%s:%u%s\n", config.host.c_str(), static_cast<unsigned>(config.port),
                listener.path().c_str());
    std::fflush(stdout);
    listener.run([] { return g_stop != 0; });
    return 0;
}
