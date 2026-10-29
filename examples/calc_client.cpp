// A client for calc_server: calls the Calc service the same way the C#, Python and TypeScript examples do (their output is
// the same, which examples/run_examples.cmake checks).
//
//   calc_client [ws://127.0.0.1:8080/rpc] [--token SECRET]
#include <cstdio>
#include <cstring>
#include <vector>

#include "calc.rpc.hpp"

using namespace sample::calc;

int main(int argc, char** argv) {
    std::string url = "ws://127.0.0.1:8080/rpc";
    rpc::ClientOptions options;
    for (int i = 1; i < argc; ++i) {
        if (!std::strcmp(argv[i], "--token") && i + 1 < argc)
            options.token = argv[++i];
        else
            url = argv[i];
    }
    try {
        CalcClient calc(rpc::Client::connect(url, options));

        std::printf("40 + 2 = %g\n", calc.Add(40, 2));

        const std::vector<double> values{1.5, 2.5, 3.0, 4.0};
        std::printf("sum = %g\n", calc.Sum(values));

        auto echo = calc.Echo("hello, rpc");                  // a view into the received message
        std::printf("echo: '%.*s'\n", static_cast<int>(echo->result().size()), echo->result().data());

        const Point square[] = {{3, 0}, {3, 4}};
        std::printf("path = %g\n", calc.PathLength({0, 0}, square));

        for (auto ticks = calc.Count(1, 5); ticks.next();)
            std::printf("  %.*s\n", static_cast<int>(ticks->label().size()), ticks->label().data());

        // client streaming: send many items, then ask for the answer
        auto total = calc.Total();
        for (int i = 1; i <= 100; ++i)
            total.write([&](CalcTotalItem::Builder& item) { item.setItem(i); });
        std::printf("total = %g\n", total.finish()->result());

        // bidirectional: each answer follows its question
        auto running = calc.Running();
        std::printf("running:");
        for (double v : {1.0, 2.0, 3.0, 4.0}) {
            running.write([&](CalcRunningItem::Builder& item) { item.setItem(v); });
            running.next();
            std::printf(" %g", running->result());
        }
        std::printf("\n");
        running.closeSend();

        // a client stream with a parameter in the call itself
        auto upload = calc.Upload("blob");
        const std::vector<uint8_t> chunk(4096, 7);
        for (int i = 0; i < 16; ++i)
            upload.write([&](Chunk::Builder& c) { c.setData(chunk.data(), chunk.size()); });
        std::printf("upload = %llu\n", static_cast<unsigned long long>(upload.finish()->result()));

        try {
            calc.Div(1, 0);
        } catch (const rpc::Error& error) {
            std::printf("expected failure: %s\n", rpc::codeName(error.code()));
        }
    } catch (const rpc::Error& error) {
        std::fprintf(stderr, "error: %s\n", error.what());
        return 1;
    }
    return 0;
}
