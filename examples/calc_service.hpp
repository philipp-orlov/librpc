// The Calc service of calc.bufs implemented: shared by the calc_server example and the tests.
#pragma once

#include <chrono>
#include <cmath>
#include <cstring>
#include <thread>

#include "calc.rpc.hpp"

namespace sample::calc {

// Handlers are plain functions of their parameters. Strings and vectors arrive as views into the request
// (no copy), and a handler reports an error by throwing rpc::Error.
class CalcImpl : public CalcService {
public:
    double Add(rpc::ServerContext&, double a, double b) override { return a + b; }
    double Sub(rpc::ServerContext&, double a, double b) override { return a - b; }

    double Div(rpc::ServerContext&, double a, double b) override {
        if (b == 0)
            throw rpc::Error(rpc::Code::InvalidArgument, "division by zero");
        return a / b;
    }

    double Sum(rpc::ServerContext&, bufs::Vector<double> values) override {
        double total = 0;
        for (double v : values)
            total += v;
        return total;
    }

    void Count(rpc::ServerContext& context, int32_t from, int32_t to, int32_t step, uint32_t delayMs,
               uint32_t payloadBytes, rpc::ServerWriter<Tick>& writer) override {
        if (step == 0)
            step = 1;
        // The payload is built straight into each item's message; nothing is allocated per item.
        for (int32_t n = from; step > 0 ? n <= to : n >= to; n += step) {
            const bool sent = writer.write([&](Tick::Builder& tick) {
                tick.setN(n);
                tick.setLabel("tick " + std::to_string(n));
                if (payloadBytes) {
                    auto payload = tick.initPayload(payloadBytes);
                    std::memset(payload.data(), 0xAB, payloadBytes);
                }
            });
            if (!sent || context.cancelled())
                return;
            if (delayMs)
                std::this_thread::sleep_for(std::chrono::milliseconds(delayMs));
        }
    }

    std::string_view Echo(rpc::ServerContext&, std::string_view text) override { return text; }  // a view of the request

    double PathLength(rpc::ServerContext&, const Point& origin, bufs::Vector<Point> points) override {
        Point previous = origin;
        double length = 0;
        for (const Point& p : points) {
            length += std::hypot(p.x - previous.x, p.y - previous.y);
            previous = p;
        }
        return length;
    }

    uint32_t Sleep(rpc::ServerContext& context, uint32_t millis) override {
        const auto until = std::chrono::steady_clock::now() + std::chrono::milliseconds(millis);
        while (std::chrono::steady_clock::now() < until) {
            if (context.cancelled())
                throw rpc::Error(rpc::Code::Cancelled, "woken up");
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
        return millis;
    }

    void Ping(rpc::ServerContext&) override {}

    double Total(rpc::ServerContext&, rpc::ValueReader<CalcTotalItem, double>& values) override {
        double sum = 0;
        while (values.next())
            sum += values.value();
        return sum;
    }

    void Running(rpc::ServerContext&, rpc::ValueReader<CalcRunningItem, double>& values, rpc::Writer<double>& out) override {
        double sum = 0;
        while (values.next()) {
            sum += values.value();
            if (!out.write(sum))
                return;  // the client is gone
        }
    }

    uint64_t Upload(rpc::ServerContext&, std::string_view /*name*/, rpc::ServerReader<Chunk>& chunks) override {
        uint64_t bytes = 0;
        while (chunks.next())
            bytes += chunks->data().size();
        return bytes;
    }

    void Chat(rpc::ServerContext&, std::string_view prefix, rpc::ServerReader<Line>& lines, rpc::ServerWriter<Line>& out) override {
        std::string reply;
        while (lines.next()) {
            reply.assign(prefix);
            reply.append(lines->text());
            if (!out.write([&](Line::Builder& line) { line.setText(reply); }))
                return;
        }
    }
};

} // namespace sample::calc
