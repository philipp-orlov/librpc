// Kitchens: builds and checks the reference values of kitchen.bufs (shared by the C++ tests and the interop server).
#pragma once

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <sstream>
#include <vector>
#include <string>

#include "kitchen.rpc.hpp"

namespace test {

inline Vec referenceVec(int seed) {
    Vec v{};
    v.x = 1.5f + static_cast<float>(seed);
    v.y = 2.5f;
    v.z = -3.5f;
    v.tag = {1, 2, static_cast<uint8_t>(3 + seed)};
    v.flag = 1;
    v.big = -5 - seed;
    return v;
}

inline void fillReference(Kitchen::Builder& k) {
    k.setTag(0);
    k.setB(true);
    k.setBt(false);
    k.setI8(100);
    k.setU8(7);
    k.setI16(31000);
    k.setU16(1);
    k.setI32(2000000000);
    k.setU32(3000000000u);
    k.setI64(9007199254740993ll);
    k.setU64(18446744073709551000ull);
    k.setF32(3.25f);
    k.setF64(1e300);
    k.setColor(Color::Blue);
    k.setText("h\xc3\xa9llo \xe2\x9c\x93 w\xc3\xb6rld");
    k.setVec(referenceVec(0));
    auto inner = k.initInner();
    inner.setN(42);
    inner.setS("inner");
    const uint8_t bytes[] = {0, 1, 254, 255};
    k.setBytes(bytes, 4);
    const int8_t i8s[] = {-128, 127};
    k.setI8s(i8s, 2);
    const int32_t ints[] = {-1, 0, 2147483647};
    k.setInts(ints, 3);
    const int64_t longs[] = {-9223372036854775807ll, 0, 9223372036854775807ll};
    k.setLongs(longs, 3);
    const double doubles[] = {0.5, -1e-300};
    k.setDoubles(doubles, 2);
    auto bools = k.initBools(3);
    bools.set(0, true);
    bools.set(1, false);
    bools.set(2, true);
    auto names = k.initNames(3);
    names.set(0, "a");
    names.set(1, "");
    names.set(2, "ccc");
    const Color colors[] = {Color::Red, Color::Green, Color::Blue};
    k.setColors(colors, 3);
    auto inners = k.initInners(2);
    auto first = inners.init(0);
    first.setN(1);
    first.setS("x");
    auto second = inners.init(1);
    second.setN(2);
    second.setS("y");
    const Vec vecs[] = {referenceVec(1), referenceVec(2)};
    k.setVecs(vecs, 2);
    k.setLate(5);
}

#define KITCHEN_EXPECT(cond)                                                          \
    do {                                                                              \
        if (!(cond)) {                                                                \
            out << #cond << "; ";                                                     \
        }                                                                             \
    } while (0)

inline bool sameVec(const Vec& a, const Vec& b) {
    return a.x == b.x && a.y == b.y && a.z == b.z && a.tag == b.tag && a.flag == b.flag && a.big == b.big;
}

/// "" when `k` holds the reference values (tag 0) or the defaults (tag 1); otherwise what differs.
inline std::string checkKitchen(const Kitchen::Reader& k) {
    std::ostringstream out;
    if (k.tag() == 0) {
        KITCHEN_EXPECT(k.b());
        KITCHEN_EXPECT(!k.bt());
        KITCHEN_EXPECT(k.i8() == 100);
        KITCHEN_EXPECT(k.u8() == 7);
        KITCHEN_EXPECT(k.i16() == 31000);
        KITCHEN_EXPECT(k.u16() == 1);
        KITCHEN_EXPECT(k.i32() == 2000000000);
        KITCHEN_EXPECT(k.u32() == 3000000000u);
        KITCHEN_EXPECT(k.i64() == 9007199254740993ll);
        KITCHEN_EXPECT(k.u64() == 18446744073709551000ull);
        KITCHEN_EXPECT(k.f32() == 3.25f);
        KITCHEN_EXPECT(k.f64() == 1e300);
        KITCHEN_EXPECT(k.color() == Color::Blue);
        KITCHEN_EXPECT(k.text() == "h\xc3\xa9llo \xe2\x9c\x93 w\xc3\xb6rld");
        KITCHEN_EXPECT(sameVec(k.vec(), referenceVec(0)));
        KITCHEN_EXPECT(k.hasInner() && k.inner().n() == 42 && k.inner().s() == "inner");
        KITCHEN_EXPECT(k.bytes().size() == 4 && k.bytes()[0] == 0 && k.bytes()[2] == 254 && k.bytes()[3] == 255);
        KITCHEN_EXPECT(k.i8s().size() == 2 && k.i8s()[0] == -128 && k.i8s()[1] == 127);
        KITCHEN_EXPECT(k.ints().size() == 3 && k.ints()[0] == -1 && k.ints()[2] == 2147483647);
        KITCHEN_EXPECT(k.longs().size() == 3 && k.longs()[0] == -9223372036854775807ll && k.longs()[2] == 9223372036854775807ll);
        KITCHEN_EXPECT(k.doubles().size() == 2 && k.doubles()[0] == 0.5 && k.doubles()[1] == -1e-300);
        KITCHEN_EXPECT(k.bools().size() == 3 && k.bools()[0] && !k.bools()[1] && k.bools()[2]);
        KITCHEN_EXPECT(k.names().size() == 3 && k.names()[0] == "a" && k.names()[1].empty() && k.names()[2] == "ccc");
        KITCHEN_EXPECT(k.colors().size() == 3 && k.colors()[0] == Color::Red && k.colors()[2] == Color::Blue);
        KITCHEN_EXPECT(k.inners().size() == 2 && k.inners()[0].n() == 1 && k.inners()[1].s() == "y");
        KITCHEN_EXPECT(k.vecs().size() == 2 && sameVec(k.vecs()[0], referenceVec(1)) && sameVec(k.vecs()[1], referenceVec(2)));
        KITCHEN_EXPECT(k.late() == 5);
    } else {
        KITCHEN_EXPECT(!k.b());
        KITCHEN_EXPECT(k.bt());
        KITCHEN_EXPECT(k.i8() == -3);
        KITCHEN_EXPECT(k.u8() == 200);
        KITCHEN_EXPECT(k.i16() == -1000);
        KITCHEN_EXPECT(k.u16() == 65000);
        KITCHEN_EXPECT(k.i32() == -123456);
        KITCHEN_EXPECT(k.u32() == 4000000000u);
        KITCHEN_EXPECT(k.i64() == -9007199254740993ll);
        KITCHEN_EXPECT(k.u64() == 18446744073709551615ull);
        KITCHEN_EXPECT(k.f32() == 1.5f);
        KITCHEN_EXPECT(k.f64() == -2.25);
        KITCHEN_EXPECT(k.color() == Color::Green);
        KITCHEN_EXPECT(k.text().empty());
        KITCHEN_EXPECT(!k.hasInner());
        KITCHEN_EXPECT(k.bytes().size() == 0 && k.ints().size() == 0 && k.names().size() == 0 && k.vecs().size() == 0);
        KITCHEN_EXPECT(k.late() == 99);
    }
    return out.str();
}

class KitchensImpl : public KitchensService {
public:
    rpc::Status Canned(rpc::ServerContext&, const Pick::Reader& request, Kitchen::Builder& response) override {
        if (request.which() == 0)
            fillReference(response);
        else
            response.setTag(1);
        return {};
    }

    rpc::Status Check(rpc::ServerContext&, const Kitchen::Reader& request, Verdict::Builder& response) override {
        const std::string problems = checkKitchen(request);
        response.setOk(problems.empty());
        response.setMessage(problems);
        return {};
    }
};

inline std::string describe(bool b, int64_t big, float f, Color c, std::string_view s, const Vec& v, bufs::Vector<int32_t> nums,
                            bufs::StringVector names, bufs::BoolVector flags, bufs::Vector<Vec> vecs, const Inner::Reader& inner,
                            double d, bool hasInner) {
    std::ostringstream o;
    o << "b=" << b << " big=" << big << " f=" << f << " c=" << static_cast<int>(c) << " s=" << s << " v=" << v.x << "," << v.y
      << "," << v.z << "," << static_cast<int>(v.tag[2]) << "," << static_cast<int>(v.flag) << "," << v.big << " nums=";
    for (size_t i = 0; i < nums.size(); ++i)
        o << (i ? "," : "") << nums[i];
    o << " names=";
    for (size_t i = 0; i < names.size(); ++i)
        o << (i ? "|" : "") << names[i];
    o << " flags=";
    for (size_t i = 0; i < flags.size(); ++i)
        o << (flags[i] ? '1' : '0');
    o << " vecs=" << vecs.size() << " inner=";
    if (hasInner)
        o << inner.n() << ":" << inner.s();
    else
        o << "none";
    o << " d=" << d;
    return o.str();
}

class NaturalImpl : public NaturalService {
public:
    std::string_view Describe(rpc::ServerContext&, bool b, int64_t big, float f, Color c, std::string_view s, const Vec& v,
                              bufs::Vector<int32_t> nums, bufs::StringVector names, bufs::BoolVector flags, bufs::Vector<Vec> vecs,
                              Inner::Reader inner, double d) override {
        // A handler's view must outlive the return statement: here, a per-thread buffer.
        thread_local std::string text;
        text = describe(b, big, f, c, s, v, nums, names, flags, vecs, inner, d, true);
        return text;
    }

    rpc::Span<const int64_t> Squares(rpc::ServerContext&, uint32_t n) override {
        thread_local std::vector<int64_t> values;
        values.clear();
        for (uint32_t i = 1; i <= n; ++i)
            values.push_back(static_cast<int64_t>(i) * i);
        return values;
    }

    rpc::Span<const std::string_view> Words(rpc::ServerContext&) override {
        static const std::string_view words[] = {"alpha", "", "gamma"};
        return words;
    }

    bool Flip(rpc::ServerContext&, bool b) override { return !b; }
    Vec Origin(rpc::ServerContext&) override { return referenceVec(0); }

    void Make(rpc::ServerContext&, std::string_view name, Inner::Builder& result) override {
        result.setN(static_cast<int32_t>(name.size()));
        result.setS(name);
    }

    void Countdown(rpc::ServerContext&, int32_t from, rpc::Writer<std::string_view>& writer) override {
        for (int32_t n = from; n >= 1; --n) {
            const std::string text = "t-minus " + std::to_string(n);
            if (!writer.write(text))
                return;
        }
    }

    void Colors(rpc::ServerContext&, rpc::Writer<Color>& writer) override {
        for (Color c : {Color::Red, Color::Green, Color::Blue})
            if (!writer.write(c))
                return;
    }

    void Nothing(rpc::ServerContext&) override {}

    void Tally(rpc::ServerContext&, uint32_t n, rpc::ServerWriter<Inner>& writer, Verdict::Builder& trailer) override {
        for (uint32_t i = 1; i <= n; ++i)
            if (!writer.write([&](Inner::Builder& item) {
                    item.setN(static_cast<int32_t>(i));
                    item.setS("item");
                }))
                return;
        trailer.setOk(true);
        trailer.setMessage(std::to_string(n) + " items");
    }

    void Fail(rpc::ServerContext&, uint32_t detail) override { throw rpc::Error(rpc::Code::Aborted, "failed on purpose", detail); }

    uint32_t Hang(rpc::ServerContext& context) override {
        struct Latch {
            std::mutex mutex;
            std::condition_variable cv;
            bool released = false;
        };
        auto latch = std::make_shared<Latch>();
        context.onCancel([latch] {
            std::lock_guard<std::mutex> lock(latch->mutex);
            latch->released = true;
            latch->cv.notify_all();
        });
        std::unique_lock<std::mutex> lock(latch->mutex);
        const bool released = latch->cv.wait_for(lock, std::chrono::seconds(10), [&] { return latch->released; });
        lock.unlock();  // the hook takes this mutex under librpc's hook lock, which onCancel({}) takes: never hold it here
        context.onCancel({});
        hooksRun += released ? 1 : 0;
        return released ? 1 : 0;
    }

    std::atomic<int> hooksRun{0};
};

} // namespace test
