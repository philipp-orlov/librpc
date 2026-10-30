# librpc design

Status: implemented and tested; [libsql](../../libsql) runs its remote databases on it (section 11).

## 1. Goals

1. A service is written once, as signatures in a libbufs schema; `rpcc` writes the messages, the C++ server and client
   stubs (header only) and a TypeScript client from it. Callers pass and receive what the signature says.
2. Two transports with one protocol: a WebSocket route of an `http::Server` (for anything, browsers included), and a
   Unix socket plus shared-memory rings (for processes on the same machine, as fast as shared memory allows).
3. Payloads are libbufs messages, read in place on the receiving side; the transports add at most one copy per direction.
4. Nothing can grow without bound because a peer is slow, fast or hostile, and a server that runs for months does not
   allocate per call, so the heap does not fragment: every queue has a size and a defined behavior when it is full.
5. The first user is libsql's remote layer (section 11); what a stateful, streaming, interruptible service needs
   shaped the protocol and the hooks.

Non-goals for now: client and bidirectional streaming, reconnect, load balancing, service discovery, compression.

## 2. Pieces

```
 schema.bufs ──► rpcc ──► schema.bufs.hpp   messages (what bufsc writes), request/response tables included
 (rpc_service)       ├──► schema.rpc.hpp    <Svc>Service, <Svc>Client, <Svc>Methods                  (header only)
                     ├──► schema.bufs.ts    TypeScript messages and <Svc>Client                       (--ts)
                     └──► schema.server.cpp / schema.client.cpp                                       (--skeleton)

 core:   Session (server), ClientImpl (client)  -- the protocol: frames, calls, windows, cancellation
 links:  ServerLink / ClientLink                -- what the core needs of a transport: whole frames, in order
           WebSocket: libhttp http::Server route / http::WebSocketClient
           Local:     Unix socket (handshake, peer credentials, "is the peer alive") + bufs::shm::Channel (two rings)
```

`rpcc` is libbufs's `bufsc` with services: it links bufsc's parser and C++ generator rather than copying them. The
parser change is small and lives in libbufs: `rpc_service` blocks are parsed into `Schema::services`, parameter lists are
lowered to tables (section 3), `bufsc` itself ignores services. `rpcc` writes `<stem>.bufs.hpp` as `bufsc` would.

A method is identified on the wire by `FNV-1a-32("namespace.Service.Method")` (`rpc::methodId`); `rpcc` fails if two
methods of a schema collide. Renaming a service or method changes its id: names are part of the contract.

## 3. From signatures to messages

`Name(a: T, b: U = default): R` is lowered by the parser, in libbufs, before layout:

* the parameters become the fields of table `<Service><Name>Request`, in order, defaults included (libbufs stores scalars
  XOR their default, so a parameter a client leaves out reads as its default, and **new parameters can be added at the
  end** of a method without breaking old clients);
* a result that is a table is the response itself; any other result (scalar, string, struct, vector, `void`) becomes a table
  `<Service><Name>Response` with one field `result` (none for `void`);
* `stream T` as the result makes it a server-streaming method whose items are T (or the wrapper above); `name: stream T` as
  the **last parameter** makes it client-streaming (the other parameters stay in the request table that opens the call; the
  items are `<Service><Name>Item`, a one-field wrapper, unless T is a table), and both together make it bidirectional; `(trailer: "X")` adds a closing
  message of table X to the stream; `(inline)` is a hint for local connections (section 5).

Because the tables are real definitions in the schema, the table form `Name(Request):Response` and the natural form mix
freely, and the lowered tables are in the generated header (`CalcAddRequest::Builder`) for anyone who wants to build a
request by hand and use the untyped `Client::call`.

The generators map types as the README's table says; the choices that matter for speed: a **server handler's parameters
are the Reader types** (`std::string_view`, `bufs::Vector<T>`, `const Struct&`, `Table::Reader`: views into the request
frame, which the call keeps alive), a **handler's results are views too** (`std::string_view`, `rpc::Span<const T>`: they are
copied into the response when the handler returns, so they may point into the request or the service but not at a local),
and a **client's results are `rpc::Response` objects** that own the received frame (strings and vectors are read in
place) except scalars and structs, which come back by value. Table parameters are passed as a function that fills the
builder (`rpc::Fill<T>`), which avoids building a table first and copying it.

## 4. Wire protocol (`include/rpc/wire/rpc.bufs`, sub-protocol `librpc.v1`)

Every message of the transport is a **frame**: one libbufs `Envelope`, optionally followed by one more message (the call's
payload), each 8-aligned and sized by its own header.

```
client -> server   Envelope [request message]
server -> client   Envelope [response message]
```

| Kind | Direction | Meaning |
| --- | --- | --- |
| `Hello` | C->S | First frame. `protocol` (1), `peer`, `token`, `window`, `max_message`. |
| `Welcome` | S->C | `peer`, `session`, the effective `window` and `max_message`, `max_inflight`. |
| `Call` | C->S | `id` (client's, nonzero), `method`, `deadline_ms`; the request message follows. |
| `Data` | both | One item of the stream of call `id` (S->C: a result; C->S: an upload); the item message follows. |
| `End` | S->C | Call `id` is over: `code`, `message`, `detail`. A unary call's response follows an Ok `End`; so does a stream's trailer and the answer of a client-streaming call. `id` 0: the Hello was refused. |
| `End` | C->S | The client has sent its last item of call `id` (no more `Data`); the call is still open until the server's `End`. |
| `Cancel` | C->S | Abandon call `id`. |
| `Credit` | both | `credit` bytes of `Data` frames were consumed (flow control): C->S for results, S->C for uploads. |
| `Ping`/`Pong` | either | Keep-alive for clients that cannot send WebSocket pings (browsers). |
| `Goodbye` | C->S | Finish what is running, then close. |
| `Closing` | S->C | The server is shutting down; new calls will be refused. |

A unary call is one frame each way. A client-streaming call is `Call`, any number of client `Data`, a client `End`, and the
server's `End` carrying the answer; a bidirectional one interleaves the two directions freely. A server-streaming call is `Call`, any number of `Data`, then an `End` (with the trailer message
behind it if the method has one). Calls are independent: they may end in any order. `detail` is an application-defined
number that travels with a status (libsql: its `ErrorCode` and `Constraint`). Every default in `rpc.bufs` is zero, so
a hand-written reader can ignore libbufs's XOR; the generated TypeScript reader implements it anyway.

Text frames are a protocol error (WebSocket close 1003). Close codes: 1000 normal, 1001 shutdown, 1002 protocol error,
1003 text frame, 1008 policy (authentication, a client that stopped reading), 1009 message too big.

### Handshake and authentication

The WebSocket route offers sub-protocol `librpc.v1` (a handshake without it is refused by libhttp with 400); the local
socket sends an `IpcHello` (magic, version, ring size, or an HTTP-style refusal status) with the channel's descriptor.
Authentication has two stages:

1. `EndpointOptions::authorize(Handshake) -> Principal | status` sees the headers, route parameters and peer address
   (and, for a local connection, `pid`/`uid`/`gid` from `SO_PEERCRED`). A status other than 0 refuses the connection
   with it (the client reports 401 as `Unauthenticated`, 403 as `PermissionDenied`). Status 0 defers the decision.
2. `EndpointOptions::authenticate(token) -> optional<Principal>` sees the `token` of `Hello`. Browsers cannot set
   headers on a WebSocket, so this is how they authenticate.

With `requireAuth`, a session with no principal after both is answered with `End{id 0, Unauthenticated}` and closed.

## 5. Transports

Both ends of the protocol core talk to a link (`rpc/detail/link.hpp`): `send(envelope, body)` of one frame from any thread
with the order kept, `close`, and for the client `receive`. The core never knows which transport it is on.

### WebSocket

The server link queues frames per connection and writes them from the connection's loop thread (at most one `post()` per
burst, with a `this`-only capture so `std::function` keeps it inline); frames are built in the server's `http::BufferPool`
and libhttp writes those of 8 KiB or more from the block itself. The client link wraps `http::WebSocketClient` and has a
reader thread. An incoming frame is copied once, into an aligned pooled block that the call (server) or the response
(client) then keeps: the request is read where it landed.

### Local socket

```
client                                            server (Endpoint::listenLocal)
connect(2) to the socket file  ───────────────►  accept; SO_PEERCRED; authorize hook
                               ◄───────────────  IpcHello{ringBytes, status} + memfd of a new bufs::shm::Channel (SCM_RIGHTS)
adopt the channel (seals checked)                 reader thread for this connection
Hello / Welcome / Call / ...: frames written to the rings, read from the rings
the socket stays open: a hang-up means the peer died
```

* **Rings.** libbufs's SPSC byte rings in anonymous, sealed shared memory. A frame is written into a reserved slot
  (`memcpy` of envelope and payload, then one release store), read by the other side, copied out into a pooled block
  and popped. Copying out is deliberate: the peer is another process and may change bytes under a reader, so the frame is
  verified on a private copy. Producers share a ring through a mutex (the critical section is a memcpy). Waiting is a
  short spin (`ipcSpin`, 20 us) and then a futex in the shared memory, so a busy connection makes no system call at all.
* **Sizing.** A ring is a power of two such that half of it holds `maxMessage` plus an envelope. Pages are touched as the
  write position walks round the ring and then stay resident: a connection costs up to two rings, flat after the first lap.
* **Who reads.** The server has one reader thread per local connection (it runs `Session::onFrame`). The client has **no**
  reader thread: the threads that wait for replies take turns reading the ring (leader/follower). A caller that has sent
  its request polls the ring, dispatches whatever arrives (its own reply, or another caller's, whose condition variable it
  signals) and, when its own call is done, wakes a waiter to take over. A reply is thus handled on the thread that wants
  it, with no hand-off. A client that is not in a call does not read; what the server pushes meanwhile waits in the
  ring (bounded by the window) until the next call or `connected()` looks.
* **Inline handlers.** A method marked `(inline)` (or all unary methods with `EndpointOptions::inlineUnary`) runs on the
  connection's reader thread instead of a worker: no queueing, no hand-off, the response goes out on the same thread. A handler
  that blocks holds up that connection's other calls; streaming calls always use a worker.
* **Failure.** Either side polls the socket for `POLLRDHUP` in the gaps between waits (50 ms slices), after draining the ring,
  so a dead peer is noticed and its in-flight calls fail with `Unavailable`; a writer waiting for room gives up after
  `stallTimeout` and drops the connection. A corrupt ring (`CorruptRing`) is treated as a dead peer.

## 6. Server

### Threads

```
 transport thread (loop / local reader)               endpoint workers (http::ThreadPool)
 -------------------------------------               -----------------------------------
 onFrame(frame):                                      runCall(call):
   Hello / Credit / Cancel / Ping / Goodbye: handle     verify the request (type id, bufs::verify)
   Call: look up the method, take a pooled              handler(context, request, response|writer)
         ServerCall, schedule it                        End frame (+ response) -> link
                                                        stream item: wait for window, frame -> link
```

`workerSpin` (default off) lets an idle worker poll for the next call instead of sleeping, which removes the wake-up from
a call's latency but needs a spare core per spinning worker: on a 2-core VM it made libsql's local round trip slower.

A WebSocket loop thread never runs a handler, never waits, and never builds a payload. Handlers run on the endpoint's own pool
(`workers`, default 8; or `executor()` for the application's) because a streaming call holds a worker for its life; the
`http::Server`'s own pool is not used so RPC cannot starve the application's handlers or the other way round. A call that
cannot be queued ends `Unavailable`; a session at `maxInflightPerSession` gets `ResourceExhausted`.

With `serialPerSession` a session's calls are queued and run one at a time in arrival order; the next starts when the
previous finishes (a streaming call holds the session until it ends). `Cancel` still reaches a running call and a queued one.

### Per-session state

`ServerContext::setSessionState(shared_ptr<void>)` / `sessionState<T>()` keep an application's state with the connection (a
database handle, say). When the connection ends the state is handed to `EndpointOptions::onSessionClose` **on a worker**,
never on the transport thread that noticed, so tearing it down may take time. Handlers should hold their own reference for
the duration of a call: the connection may end while a call is still running.

### Flow control

libhttp closes a WebSocket connection whose queued output passes `maxWebSocketQueuedBytes`, and a ring has finite room; a
producer that streams faster than the consumer reads would trigger either, so stream `Data` frames are gated by a window:

* the server tracks `unacked`, the bytes of `Data` frames sent and not yet credited;
* `StreamWriter::send` waits while `unacked + frame > window` (a frame larger than the window is sent when `unacked` is 0, so
  nothing can wedge); it returns false if the call is cancelled, the deadline passes or the connection closes;
* the client counts the bytes of `Data` frames as its caller consumes them and sends `Credit` once a quarter window has
  accumulated, or at once when it has read everything queued (otherwise a small frame followed by one that does not fit
  in the rest of the window would wait for credit that never comes); items dropped by a cancelled call, and `Data` for calls that no longer exist, are credited too;
* the window is the smallest of the client's `Hello.window`, `EndpointOptions::window` (1 MiB) and, for WebSockets, half
  of the `http::Server`'s `maxWebSocketQueuedBytes`;
* a client that stops reading for `stallTimeout` (30 s) is cut: close 1008, and the call ends `Unavailable`.

Uploads mirror this: the client counts the bytes of the `Data` frames it sent and waits while `unacked + frame > window`; the
server queues the items of a call in a per-call inbox (a ring that keeps its capacity), `ServerReader::next` pops one and
credits it, and a cancelled or finished call credits whatever it never read. A handler that stops reading therefore stops
the sender, and a sender that overruns its window (more than twice the window plus a frame queued for one call) is cut: close 1008. A
handler that returns before the client has finished ends the call; what was queued and what still arrives is dropped and
credited, so the client's window is whole again.

The window is per connection, shared by its streams. Unary responses are not gated: they are bounded by
`maxInflightPerSession` times the maximum message size.

### Cancellation, deadlines, hooks

`Call.deadline_ms` becomes a deadline on the server's clock when the frame arrives; the client enforces the same deadline
itself and answers `DeadlineExceeded` without waiting. `ServerContext::cancelled()` is true once the client sent `Cancel`, the
deadline passed, the connection closed or the server is shutting down past its grace. Work that cannot poll (a database
statement) registers `ServerContext::onCancel(hook)`: the hook runs on whichever thread notices, under a lock that
`onCancel({})` also takes, so after the handler clears it the hook is not running and never will be. A call cancelled
because the server is closing ends `Unavailable`, not `Cancelled`.

### Shutdown

`Endpoint::shutdown(grace)`: refuse new connections (503) and calls (`Unavailable`), wake and join the accept thread,
send `Closing` to every session, wait up to `grace` for running calls, cancel what is left, wait up to 2 s more, close the
sessions (1001), join the local reader threads and the workers. A second caller (the destructor) waits for the first.
A handler that never returns after `cancelled()` becomes true blocks the join: handlers must poll or hook it.

### Errors

A handler throws or returns a status (table form). `rpc::Error` carries a chosen status; any other exception is `Internal`
with the message `internal error` (the text of `what()` is for the server's log, not the client, unless `exposeExceptions`).
A request that does not verify is `InvalidArgument`; a request of the wrong type, `InvalidArgument`; an unknown method,
`Unimplemented`. A status message is cut to 1 KiB.

## 7. Client

One `rpc::Client` is a handle to one connection. Calls from any number of threads are multiplexed; each caller blocks on its
own call's condition variable. Call state lives in recycled slots (a slot index is part of the call id, so a reply finds its
call in O(1) and a late reply for a freed slot is recognized and dropped, and credited if it carried stream data).

* A response is the frame it arrived in: the body is a `Payload` that owns the pooled block and reads in place.
* A stream's items queue in a ring that keeps its capacity; `next()` pops one, credits it, and hands it to the caller.
* A request larger than the server's `max_message` (from `Welcome`) fails locally with `ResourceExhausted`.
* `cancelAll()` cancels everything in flight from any thread; `notify()` sends a call whose answer nobody wants (the slot is
  freed when the answer arrives); `close()` says goodbye and joins.
* WebSocket connections have a reader thread; `http::WebSocketClient::close()` wakes it from another thread. Local
  connections have none (section 5).
* Connection loss ends every call with `Unavailable` and the client is finished; there is no reconnect.

## 8. TypeScript

`ts/src/bufs.ts` is the libbufs reader (`Table`) and builder (`MessageBuilder`) that generated code is written against;
`ts/src/client.ts` the client; `ts/src/rpc.bufs.ts` the envelope codec, itself generated by `rpcc` from `rpc.bufs` (the
`rpc_ts_wire_current` test fails if it is stale).

* A generated table is a class of lazy getters over the received bytes plus `T.encode(init)`. 64-bit integers are `bigint`,
  enums TypeScript enums, `[ubyte]` is `Uint8Array`, a missing table is `null`.
* A natural-form method is `async` with positional parameters (trailing defaults become optional) and returns the plain
  value; a stream is an `AsyncIterable` (with `.trailer` when the method has one); `{ timeoutMs, signal }` set a deadline and
  cancellation. Errors are `RpcError` with `code` and `detail`.
* There is no verifier. `DataView` throws `RangeError` on any read outside the message, so a malformed message cannot expose
  memory; it can throw when a getter is called.
* Browsers cannot send WebSocket pings: the client sends `Ping` envelopes every 15 s so the server's idle timeout does not
  close an idle connection. The stream iterator is the consumer's pace: `Credit` is sent as `next()` hands items out.

## 8b. C#

`cs/Librpc` is a small .NET library (net10.0) over libbufs's `Bufs` project (`libbufs/cs/Bufs`: the reader, `Table`, a struct over
`ReadOnlyMemory<byte>` with the same lazy getters as the other languages, and the builder, `MessageBuilder`): `Client.cs` the client
(`RpcClient.ConnectAsync`, a read loop on the thread pool, a ping loop, credit accounting) and `rpc.bufs.cs` the envelope codec,
generated by `rpcc --cs` from `rpc.bufs`. Generated code has the shape of the TypeScript: tables are `readonly struct` readers
with `Decode`/`Encode`, `...Init` classes with nullable members for building (a member left null keeps its default), service
clients with `PascalAsync` methods returning `Task<T>`, `RpcStream<T>` (an `IAsyncEnumerable`, with `.Trailer` when the method
has one), `RpcUpload<I,R>` and `RpcDuplex<I,O>`. Deadlines and cancellation are `CallOptions { Timeout, CancellationToken }`;
failures are `RpcException` with `Code` and `Detail`. `cs/Librpc.Tests` runs the generated clients of the example schemas against
`tests/interop_server` (`rpc_csharp`).

## 8c. Python

`python/librpc` is an asyncio client with no dependencies: `websocket.py` a small RFC 6455 client (binary messages, `ws://` and
`wss://`, masking, ping/pong, close), `client.py` the client with the logic of the TypeScript one (call table, credit accounting
for both directions, deadlines through `loop.call_later`, ping loop, `Closing`), and `rpc_bufs.py` the envelope codec, generated by
`rpcc --py` from `rpc.bufs` (the `rpc_py_wire_current` test fails if it is stale). The messages use libbufs's Python runtime
(`libbufs/python/bufs`: `Table` over a `memoryview`, `MessageBuilder`). Generated clients are `async` methods for unary calls and
plain methods returning `RpcStream`, `RpcUpload` or `RpcDuplex` for streams; cancelling the awaiting task, leaving an `async with`
over a stream, or a deadline sends `Cancel`. Method names are `snake_case`; a table argument is a dict.

## 9. Limits (defaults)

| Limit | Default | Where |
| --- | --- | --- |
| Message | libhttp `maxWebSocketMessageBytes`, 1 MiB; `EndpointOptions::maxMessage` raises it (up to 16 MiB on a WebSocket route); also sizes the local rings | both directions |
| Sessions | 1024 | `maxSessions` (503 beyond) |
| Calls in flight per session | 64 | `maxInflightPerSession` |
| Workers / queue | 8 / 1024 | `workers`, `queueCapacity` |
| Stream window | 1 MiB, at most half of `maxWebSocketQueuedBytes` | `window` |
| Idle connection | 5 min | `idleTimeout` (any frame resets it) |
| Stalled stream / full ring | 30 s | `stallTimeout` |
| Status message | 1 KiB | |

## 10. Memory discipline

* **Messages and frames** come from `bufs::BufferPool` (and libhttp's pool for WebSocket frames): size-classed blocks carved
  from slabs, which cannot fragment and whose footprint follows the peak number of blocks in flight.
* **Calls.** Server `ServerCall` objects are recycled through a free list and reference counted by hand (`CallRef`) because
  `shared_ptr` would allocate; a session's call table is a vector reserved to `maxInflight`. Client call slots are recycled
  and addressed by index.
* **Tasks.** `std::function` stores a callable inline only if it is small *and trivially copyable*, so everything posted to a
  pool or a loop captures raw pointers (with a hand-held reference) rather than `shared_ptr`s.
* **Envelopes** are built in a thread-local pooled builder.
* **Verification.** `tests/test_alloc.cpp` replaces `operator new` and counts allocations on every thread across 20,000 calls
  of each kind -- scalar unary, string unary, unary on a worker, a 20-item stream -- over both transports (0 per call; a
  handful at most over the whole run, from recycled containers growing to their high-water mark), and checks that resident
  memory is flat over 100,000 calls.

What does allocate: a status that carries a message (errors), the first use of a connection or a call slot, and
whatever application code does.

## 11. libsql on librpc

libsql's remote layer (`SqlSocket`, `Database::connect`, the `sql` tool's web interface) is a librpc service, `Sql`, written
as signatures in `libsql/include/sql/wire/remote.bufs`. It is the user the design was tested against, and what it needed is
in librpc as general features:

| libsql needs | librpc |
| --- | --- |
| a database handle, prepared statements and transactions per connection | session state + `onSessionClose` on a worker |
| a session's calls in order, one at a time | `serialPerSession` |
| rows streamed as `ResultSet` batches, flow-controlled | `stream`, the window, `ServerWriter::write(ptr, size)` of a message built elsewhere |
| counters after a statement | `(trailer: "StatementEnd")` |
| the exact `ErrorCode` and `Constraint` across the wire | `Status::detail` |
| interrupt a running statement (cancel, timeout, disconnect, shutdown) | `ServerContext::onCancel` hook; `Client::cancelAll()` |
| fire-and-forget close of a statement or transaction from a destructor | `Client::notify` |
| the effective message size, to size batches | `ServerContext::maxMessage()` |
| the same authorize / authenticate / requireAuth / limits it had | `EndpointOptions` |
| clients on the same machine, fast | the local transport: `sql unix:///path`, `Database::connect("unix://...")` |

Replacing libsql's hand-written protocol took out roughly 4,600 lines there (the framing, the ring queues, the credit
accounting, the TypeScript codecs and their golden tests, the protocol-level test) and put in about 1,900 (the service
schema, the session logic, the new service tests; the generated TypeScript is not counted) while keeping its behavior: its remote test suites pass unchanged on the new
transport, including the allocation budget, interrupts, reconnects and TLS.

## 12. Changes outside this directory

* `libbufs/bufsc`: `rpc_service` blocks are parsed (table form and natural form, `stream`, `void`, attributes) and lowered to
  tables; `bufsc` itself generates nothing for services. Tests, README row. `cppAccessorName`/`cppPrefixedName` are exported
  for `rpcc`.
* `libhttp`: `http::WebSocketClient::close()` is now safe against a thread blocked in `receive()` or sending (it shuts the
  socket down and leaves releasing the descriptor to the destructor or the next `connect()`), which ThreadSanitizer showed was
  a data race before; this is also what lets a client wake its reader thread at once.
* `libsql`: the remote layer moved onto librpc (its own README and design notes describe it).
