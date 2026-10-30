// The server the TypeScript tests run against: Calc and Kitchens on one port.
//   interop_server PORT [--window BYTES] [--token SECRET] [--local=SOCKET_PATH]
// Prints "ready" once it listens; stops on SIGTERM/SIGINT or when stdin closes.
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <thread>

#include "calc_service.hpp"
#include "kitchen_service.hpp"

namespace {
volatile std::sig_atomic_t g_stop = 0;
void onSignal(int) { g_stop = 1; }
} // namespace

int main(int argc, char** argv) {
    if (argc < 2)
        return 2;
    http::ServerConfig config;
    config.host = "127.0.0.1";
    config.port = static_cast<uint16_t>(std::atoi(argv[1]));
    config.ioThreads = 2;
    rpc::EndpointOptions options;
    std::string token;
    std::string local;
    for (int i = 2; i < argc; ++i) {
        if (!std::strcmp(argv[i], "--window") && i + 1 < argc)
            options.window(static_cast<size_t>(std::atoll(argv[++i])));
        else if (!std::strcmp(argv[i], "--token") && i + 1 < argc)
            token = argv[++i];
        else if (!std::strncmp(argv[i], "--local=", 8))
            local = argv[i] + 8;
    }
    if (!token.empty()) {
        options.requireAuth().authenticate([token](std::string_view presented) -> std::optional<rpc::Principal> {
            if (presented == token)
                return rpc::Principal{"ts-client", nullptr};
            return std::nullopt;
        });
    }
    sample::calc::CalcImpl calc;
    test::KitchensImpl kitchens;
    test::NaturalImpl natural;
    rpc::Listener listener(config, std::move(options));
    listener.add(calc).add(kitchens).add(natural);
    if (!local.empty())
        listener.listenLocal(local);
    std::signal(SIGINT, onSignal);
    std::signal(SIGTERM, onSignal);
    std::printf("ready\n");
    std::fflush(stdout);
    listener.run([] { return g_stop != 0; });
    return 0;
}
