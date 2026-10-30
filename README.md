# librpc

RPC for C++17, TypeScript, C# and Python, over WebSockets and over local sockets, on top of [libhttp](../libhttp) and
[libbufs](../libbufs). You write a service as signatures,

```
rpc_service Calc {
    Add(a: double, b: double): double;
    Count(from: int, to: int, step: int = 1): stream Tick;
}
```

and the protocol compiler `rpcc` writes the request and response messages and header-only C++ server and client
stubs and TypeScript, C# and Python clients from it. Calls take and return what they say: `calc.Add(40, 2)` is a `double`. Behind
that, requests and responses are libbufs messages that are read in place on the other side, and the call path is
built to run for months without growing: pooled messages, recycled call objects, no heap allocation per call once warm.

* `include/rpc/`, `src/`: the C++ runtime (`rpc::Listener`, `rpc::Endpoint`, `rpc::Client`, `rpc::Status`).
* `rpcc/`: the protocol compiler. It links libbufs's schema parser, so it accepts exactly what `bufsc` accepts, plus services.
* `ts/`: the TypeScript runtime (`@librpc/client`): the client, re-exporting the libbufs reader and builder (`libbufs/ts`).
* `cs/`: the C# runtime (`Librpc`: `RpcClient` over `ClientWebSocket`) and its tests (`cs/Librpc.Tests`).
* `python/`: the Python runtime (`librpc`: an asyncio `RpcClient` with its own small WebSocket client, no dependencies) and its tests.
* `examples/`: a calculator with a C++ server, and a client in each language (C++, TypeScript with a browser page, C#, Python).
* `tests/`, `bench/`: integration tests (every transport), the allocation test, and the benchmark.
* `docs/design.md`: the wire protocol, the transports, threading, flow control, limits.

## Quick start

`calc.bufs`:

```
namespace sample.calc;

table Tick { n: int; label: string; }

rpc_service Calc {
    /// a + b
    Add(a: double, b: double): double;
    /// Dividing by zero is InvalidArgument (a handler throws rpc::Error).
    Div(a: double, b: double): double;
    Sum(values: [double]): double;
    Echo(text: string): string;
    /// Streams the numbers from `from` to `to`.
    Count(from: int, to: int, step: int = 1, delay_ms: uint = 0): stream Tick;
    Reset(): void;
}
```

`rpc_service` is FlatBuffers' syntax and `Method(Request):Response` still works for a method that takes and returns
tables you have defined yourself ("the table form"); the signatures above are the natural form (section "Schema").

### Build

```cmake
add_subdirectory(librpc)                       # brings libhttp and libbufs along (RPC_HTTP_DIR, RPC_BUFS_DIR)
add_executable(calc_server calc_server.cpp)
rpc_generate(calc_server SCHEMAS calc.bufs)    # calc.bufs.hpp + calc.rpc.hpp at build time; links rpc::rpc
```

`rpc_generate(... TS)` also writes `calc.bufs.ts`; `SKELETONS` writes `calc.server.cpp` and `calc.client.cpp`, programs
that build and run, to start from. Or call it by hand: `rpcc -o out -I includes --ts --skeleton calc.bufs`.

### Serve (C++)

```cpp
#include "calc.rpc.hpp"

class CalcImpl : public sample::calc::CalcService {      // generated base: override what you implement
public:
    double Add(rpc::ServerContext&, double a, double b) override { return a + b; }

    double Div(rpc::ServerContext&, double a, double b) override {
        if (b == 0)
            throw rpc::Error(rpc::Code::InvalidArgument, "division by zero");
        return a / b;
    }

    double Sum(rpc::ServerContext&, bufs::Vector<double> values) override {   // a view into the request: no copy
        double total = 0;
        for (double v : values) total += v;
        return total;
    }

    std::string_view Echo(rpc::ServerContext&, std::string_view text) override { return text; }

    void Count(rpc::ServerContext&, int32_t from, int32_t to, int32_t step, uint32_t delayMs,
               rpc::ServerWriter<Tick>& writer) override {
        for (int32_t n = from; n <= to; n += step)
            if (!writer.write([&](Tick::Builder& tick) { tick.setN(n); }))   // false once the call is over
                return;
    }

    void Reset(rpc::ServerContext&) override {}
};

int main() {
    http::ServerConfig config;
    config.port = 8080;
    CalcImpl calc;
    rpc::Listener listener(config);          // ws://host:8080/rpc, and GET /healthz
    listener.add(calc);
    listener.listenLocal("/run/calc.sock");  // and, for programs on this machine, a Unix socket
    listener.run();                          // signal handling and shutdown: see examples/calc_server.cpp
}
```

Parameters reach a handler as views into the request (`std::string_view`, `bufs::Vector<T>`, `const Struct&`,
`Table::Reader`): nothing is copied. A handler returns scalars and structs by value, and strings and vectors as a
`std::string_view` / `rpc::Span<const T>` that is copied into the response when it returns, so it may point into the
request, the service object or static data (not at a local). Errors are exceptions: throw `rpc::Error` to choose the
status; any other exception becomes `Internal("internal error")` (`EndpointOptions::exposeExceptions` shows `what()`).
Handlers run on the endpoint's worker threads, so they may block.

An application that already has an `http::Server` attaches the same route next to its own, and an `Endpoint` also works
with no `http::Server` at all, for local clients only:

```cpp
rpc::Endpoint endpoint(rpc::EndpointOptions().authorize(checkHeaders).workers(16));
endpoint.add(calc).attach(server, "/rpc");           // beside the application's own routes
endpoint.add(calc).listenLocal("/run/calc.sock");    // or: no web server needed
endpoint.shutdown(std::chrono::seconds(5));          // while the http::Server, if any, still runs
```

### Call (C++)

```cpp
rpc::Client connection = rpc::Client::connect("ws://localhost:8080/rpc");   // or "unix:///run/calc.sock"
sample::calc::CalcClient calc(connection);       // cheap to copy; any number of threads may call

double sum = calc.Add(40, 2);                    // scalars come back by value
auto echoed = calc.Echo("hello");                // strings, vectors and tables: an rpc::Response, read in place
std::string_view text = echoed->result();        // (a view into the received message)
const double values[] = {1, 2, 3};
calc.Sum(values);                                // vectors go in as rpc::Span: no std::vector needed

for (auto ticks = calc.Count(1, 5); ticks.next();)   // a stream; trailing parameters with defaults may be left out
    use(ticks->n());

rpc::CallOptions options;
options.timeout = std::chrono::seconds(2);       // sent to the server; the client gives up at the same time
calc.Div(1, 0, options);                         // throws rpc::Error(Code::InvalidArgument, "division by zero")
```

Every call throws `rpc::Error` (`code()`, `status()`) when it does not end in `Ok`. `next()` throws if a stream ends in
an error; leaving a stream early cancels it.

### Call (TypeScript)

```ts
import { RpcClient } from '@librpc/client';
import { CalcClient } from './calc.bufs';            // rpcc --ts

const rpc = await RpcClient.connect('ws://localhost:8080/rpc', { token: 'secret' });
const calc = new CalcClient(rpc);

console.log(await calc.add(40, 2));                              // 42
console.log(await calc.echo('hello'));                           // 'hello'
for await (const tick of calc.count(1, 5)) console.log(tick.label);   // break cancels
await calc.div(1, 0, { timeoutMs: 2000, signal });               // RpcError(Code.InvalidArgument)
```

Works in browsers and in Node 22+ (any other `WebSocket` constructor can be passed in `ClientOptions`). `examples/ts/build.mjs`
builds a Node client and a browser page; `calc_server --web examples/ts/dist` serves the page and the RPC route from
one port.

### Call (C#)

```csharp
using Librpc;
using Sample.Calc;                                               // rpcc --cs  (calc.bufs.cs)

await using var rpc = await RpcClient.ConnectAsync("ws://localhost:8080/rpc", new ClientOptions { Token = "secret" });
var calc = new CalcClient(rpc);

Console.WriteLine(await calc.AddAsync(40, 2));                   // 42
await foreach (var tick in calc.CountAsync(1, 5)) Console.WriteLine(tick.Label);   // break cancels
var total = calc.TotalAsync();                                   // client streaming
for (int k = 1; k <= 100; k++) await total.SendAsync(k);
Console.WriteLine(await total.FinishAsync());                    // 5050
await calc.DivAsync(1, 0, new CallOptions { Timeout = TimeSpan.FromSeconds(2) });   // RpcException(Code.InvalidArgument)
```

Add `cs/Librpc/Librpc.csproj` as a project reference (net10.0) and the generated `*.bufs.cs` to your project
(`rpcc --cs`, or `rpc_generate(... CS)` in CMake). Readers are `readonly struct`s over `ReadOnlyMemory<byte>`: strings
and scalar vectors are read in place (`ReadOnlySpan<T>`, `<Name>Utf8`); requests and table arguments are `...Init` objects.
WebSockets only.

### Call (Python)

```python
from librpc import Code, RpcClient, RpcError
from calc_bufs import CalcClient                                 # rpcc --py  (calc_bufs.py)

async with await RpcClient.connect("ws://localhost:8080/rpc", token="secret") as rpc:
    calc = CalcClient(rpc)
    print(await calc.add(40, 2))                                 # 42
    async with calc.count(1, 5) as ticks:                        # leaving the block cancels the stream
        async for tick in ticks:
            print(tick.label)
    total = calc.total()                                         # client streaming
    for k in range(1, 101):
        await total.send(k)
    print(await total.finish())                                  # 5050
    try:
        await calc.div(1, 0, timeout=2)
    except RpcError as e:                                        # e.code == Code.InvalidArgument, e.detail
        ...
```

Python 3 with asyncio and nothing else: the WebSocket client is part of the package. Put `libbufs/python` (package `bufs`) and
`librpc/python` (package `librpc`) on `PYTHONPATH` (both directories also have a `pyproject.toml`, but installing from them has not
been tried yet). Methods are
`snake_case`; a unary call is a coroutine, a stream is returned directly and read with `async for`; messages are plain Python
values (a table argument is a dict, a table result a reader with properties). WebSockets only.

## Languages

| | Servers | Clients | Runtime | Generated by |
|---|---|---|---|---|
| C++17 | yes (`<Service>Service`, listeners for WebSocket and local sockets) | yes (blocking, any thread; local sockets too) | `include/rpc`, `src` | `rpcc` (default) |
| TypeScript | | yes (browsers and Node 22+) | [ts/](ts) with libbufs's [ts/](../libbufs/ts) | `rpcc --ts` |
| C# | | yes (`async`/`await`, .NET) | [cs/Librpc](cs/Librpc) with libbufs's [cs/Bufs](../libbufs/cs/Bufs) | `rpcc --cs` |
| Python | | yes (`asyncio`) | [python/librpc](python/librpc) with libbufs's [python/bufs](../libbufs/python/bufs) | `rpcc --py` |

All four speak the same protocol (section 4 of docs/design.md) and have the same features: unary, server-streaming,
client-streaming and bidirectional calls, deadlines, cancellation, flow control, tokens, status codes with `detail`. The messages
of every language are the ones [libbufs](../libbufs) generates with `bufsc --ts/--cs/--py`; rpcc adds the service clients on top
of the same generators. The C#, Python and TypeScript runtimes are clients; servers are written in C++.

[examples/](examples) has the calculator service ([calc.bufs](examples/calc.bufs)), a C++ server (`calc_server`) and one client per
language (`calc_client.cpp`, `python/calc_client.py`, `ts/calc_client.ts` and a browser page, `cs/CalcClient/`) that all make the same
calls and print the same text; `ctest -R rpc_examples` runs them all against the server.

## Schema

A method is a signature: `Name(param: type [= default], ...): result`. A parameter or result may be a scalar
(`bool`, `byte` ... `ulong`, `float`, `double`), an enum, a string, a struct, a table, or a vector of any of those
except tables; the result may also be `void` or `stream T`. rpcc turns the parameters into a request table
(`CalcAddRequest`) and a non-table result into a response table with one field `result` (a table result is the response
itself), so a method can grow: **add parameters at the end** and old clients keep working (a missing parameter reads as
its default). The tables are real libbufs messages in the generated header.

| | server handler gets | client call takes / returns (C++) | TypeScript |
|---|---|---|---|
| scalar, enum | by value | by value | `number`, `bigint` (64-bit), enum |
| string | `std::string_view` | `std::string_view` / `Response` (`->result()` is a view) | `string` |
| struct | `const S&` | `const S&` / by value | an interface |
| `[scalar]`, `[struct]` | `bufs::Vector<T>` | `rpc::Span<const T>` / `Response` | `number[]`, `Uint8Array` for `[ubyte]` |
| `[bool]`, `[string]` | `bufs::BoolVector`, `bufs::StringVector` | `Span<const bool>`, `Span<const std::string_view>` | arrays |
| table | `Table::Reader` | `rpc::Fill<Table>` (a function that fills it) / `Response<Table>` | `TableInit` / reader class |

**Streaming** is a `stream` in front of the result (the server sends many items), of the last parameter (the client sends
many items: `Total(values: stream double): double`), or of both (`Running(values: stream double): stream double`, answers as
the items arrive). The items of a stream of a scalar, string or struct are wrapped in a one-field table by rpcc; a stream of a
table is that table. In C++ the server handler gets a `rpc::ServerReader<T>` (or `rpc::ValueReader<Wrapper, V>` for the wrapped
kinds): `while (values.next()) use(values.value());`; the client gets a `rpc::ClientUpload<Item, Resp>` (`write(fill)`,
`finish()` returns the answer) or a `rpc::ClientDuplex<Item, Out>` (also a `ClientStream<Out>`: `write`, `closeSend`, `next`).
TypeScript has `RpcUpload` and `RpcDuplex`, C# the same names with `SendAsync`/`FinishAsync`/`EndAsync`. Uploads are flow
controlled in the same way as downloads, so a fast sender waits for a slow handler.

Method attributes, after the result: `(inline)` runs the handler on a local connection's reader thread (for methods that
are short and never block; see Local sockets); `(trailer: "T")` on a `stream` method makes the stream end with one
message of table `T`, which the handler fills in (an extra `T::Builder&` parameter) and the client reads with
`stream.trailer()` after the last item; `(streaming: "server")` is the table form's way to say `stream`.

## Local sockets

`Endpoint::listenLocal(path)` listens on a Unix socket. A client that connects (`unix:///path`) is handed an anonymous
shared-memory channel -- two rings, one each way, sealed so the peer cannot shrink them -- and from then on the same
frames as over a WebSocket travel through the rings: no system call per message while either side is busy (futex
wake-ups otherwise, after a short spin), one copy per direction, and nothing to parse beyond libbufs's verify. The
socket stays open so each side learns at once when the other process dies; the server also learns who the peer is
(`Handshake::pid/uid/gid`, from the kernel) for its `authorize` hook, and the socket file is `0600` by default.

On a 2-core arm64 VM (`bench/bench_rpc`, server in another process):

| | WebSocket (loopback) | local socket |
|---|---|---|
| `Add(a, b)` round trip, median | 64 us | **1.1 us** (p99 5 us) |
| `Add` calls per second, 8 threads on one connection | 50 k | **400 k** |
| `Echo` 4 KiB | 75 us | **5 us** |
| `Echo` 64 KiB | 190 us (0.35 GB/s) | **17 us (3.9 GB/s)** |
| stream of 16 KiB items | 0.9 GB/s | **4.2 - 5.9 GB/s** |
| stream of 100 B items | 0.04 GB/s (0.4 M items/s) | 0.2 GB/s (**2.2 M items/s**) |

What makes it fast: a client thread that waits for its reply reads the ring itself (a local connection has no reader
thread, so there is no hand-off between threads on the way back); methods marked `(inline)` run on the server's reader
thread (no hand-off on the way in); both sides spin for `ipcSpin` (20 us) before sleeping on the futex. Handlers that
block or run long should not be `(inline)`: they would hold up the other calls of their connection.

A connection's rings are sized by the endpoint's `maxMessage` (the largest message is half a ring) and are touched
page by page as the write position walks round them, so a connection costs up to two rings of resident memory and then
stays flat; lower `maxMessage` for many connections.

## GPU-visible receive buffers

A received frame is copied once, from the socket or ring, into a block; by default that is librpc's pool, which a GPU cannot
read. Set `EndpointOptions::inboundMemory(...)` (server) or `ClientOptions::inboundMemory` (client) to land that one copy in
memory the caller chose: either `rpc::InboundMemory{allocate, release, ctx}` callbacks (the caller assigns the memory, e.g. per
device), or an `rpc::PinnedPool`, a grow-only pool that registers each block once through callbacks you wire to
`cudaHostRegister` / `cudaHostUnregister` (no CUDA dependency in librpc):

```cpp
rpc::PinnedPool pool({[](void*, void* p, size_t n) { return cudaHostRegister(p, n, cudaHostRegisterDefault) == cudaSuccess; },
                      [](void*, void* p, size_t) { cudaHostUnregister(p); }, nullptr});
rpc::ClientOptions options;
options.inboundMemory = pool.memory();   // the pool must outlive every message received through it
```

C++ only (the TypeScript, C# and Python clients do not have it). See `include/rpc/memory.hpp` and design.md ("Caller-supplied
memory for received frames").

## What you get

* **Typed, header-only stubs.** `<Service>Service` (server base class; methods default to `Unimplemented`),
  `<Service>Client`, `<Service>Methods` (the method ids). Define `RPC_NO_SERVER` or `RPC_NO_CLIENT` to leave a side out.
* **Unary, server-streaming, client-streaming and bidirectional calls**, any number of concurrent calls on one connection, deadlines, cancellation
  (`ServerContext::cancelled()`, `ServerContext::onCancel(hook)` for work that cannot poll, `ServerWriter::write`
  returning false, `Client::cancelAll()`, `AbortSignal`), and one-way calls (`Client::notify`).
* **Flow control.** A stream is gated by a byte window the client replenishes as it consumes items, so a slow reader
  holds back the producer instead of filling memory (or libhttp's per-connection queue).
* **Status codes** that are gRPC's, with an application `detail` that travels with them (libsql puts its own
  error code there), on the wire and in both languages.
* **Sessions**: state kept per connection (`ServerContext::setSessionState`), a hook when the connection ends,
  and `serialPerSession` to make a session's calls run one at a time in arrival order -- what a stateful service such as a
  database session needs.
* **Authentication hooks**: `authorize` sees the handshake (headers, route parameters, peer address and, locally, pid/uid/gid)
  and may refuse with an HTTP-style status; `authenticate` sees the token of the client's first message (the only way for a
  browser to authenticate). A `Principal` reaches every handler through `ServerContext::principal()`.
* **Limits** that have a defined behavior when reached: sessions, calls in flight per session, worker queue, message size,
  window, idle and stall timeouts; `Endpoint::stats()`; an `onCall` hook for audit logs and metrics.
* **Graceful shutdown**: clients are told, running calls get a grace period, the rest are cancelled (and end `Unavailable`).
* **Runs for months.** The call path allocates nothing from the heap per call once warm (call objects and call slots are
  recycled, messages and frames come from pools, `std::function` captures are kept trivially copyable so they stay
  inline), which `tests/test_alloc.cpp` checks with a counting `operator new` on every thread, alongside a flat-RSS check.
* **Sanitized.** The C++ suite runs clean under AddressSanitizer, UBSan and ThreadSanitizer, over both transports.

## Not there yet

* Client and bidirectional streaming in the table form (use the signature form with a `stream` parameter).
* Automatic reconnect: a lost connection fails every call with `Unavailable`; connect again.
* TypeScript has no verifier: a malformed message throws `RangeError`/`MalformedMessage` when read, it cannot read outside itself.
  The TypeScript and C# clients speak WebSockets only (a browser cannot open a Unix socket; local sockets are C++ to C++).
* Vectors of tables as parameters or results of the natural form (wrap them in a table).
* `wss://` is wired (`-DRPC_WITH_TLS=ON`) but only libsql's tests exercise it.

## Tests

```
cmake -S . -B build && cmake --build build && ctest --test-dir build     # `npm install` in ts/ for the TypeScript tests
cmake -S . -B build-bench -DCMAKE_BUILD_TYPE=Release -DRPC_BUILD_BENCHMARKS=ON && build-bench/bench/bench_rpc
```

`rpc_integration` runs the same C++ clients and servers on loopback over **both transports** (unary, streaming and flow
control, cancellation, deadlines, concurrency, authentication, handler errors, bad requests, a client that stops reading,
shutdown, natural-form methods of every type, trailers, details, cancel hooks, every field type) and then a server in
another process, per-session state and serial execution, local-socket specifics (peer credentials, refusal, a dead
client). `rpc_typescript` is the TypeScript client against the C++ server, including a codec check of every field type
in both directions; `rpc_csharp` (when `dotnet` is installed) the same for the C# client, built from the `rpcc --cs` output of
the example schemas; `rpc_allocations` the allocation and memory test; `rpc_ts_wire_current` that the checked-in
`ts/src/rpc.bufs.ts` matches `rpc.bufs`; and `rpcc` rejection tests.

[libsql](../libsql) uses librpc for its remote databases (`Database::connect`, `SqlSocket`): its service is
`libsql/include/sql/wire/remote.bufs`.

## License

Apache-2.0, see [LICENSE](LICENSE).
