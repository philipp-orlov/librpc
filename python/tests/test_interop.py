# Copyright (c) 2026 Philipp Orlov

"""The Python client against the C++ server (tests/interop_server.cpp): every call kind, flow control, cancellation,
authentication and a codec check of every field type in both directions.

    python3 test_interop.py <interop_server> <gen dir>      (the generated calc_bufs.py and kitchen_bufs.py)
"""

import asyncio
import os
import socket
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, sys.argv[2])
sys.path.insert(0, os.path.join(HERE, ".."))
sys.path.insert(0, os.path.join(HERE, "..", "..", "..", "libbufs", "python"))

import calc_bufs as calc  # noqa: E402
import kitchen_bufs as kitchen  # noqa: E402
from librpc import Code, RpcClient, RpcError  # noqa: E402

checks = failures = 0


def check(ok, what):
    global checks, failures
    checks += 1
    if not ok:
        failures += 1
        print(f"  CHECK FAILED: {what}", file=sys.stderr)


async def throws(code, awaitable, what):
    try:
        await awaitable
        check(False, f"{what}: expected {code.name}, nothing raised")
    except RpcError as e:
        check(e.code == code, f"{what}: expected {code.name}, got {e.code}")


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def start(binary, port, *args):
    p = subprocess.Popen([binary, str(port), *args], stdout=subprocess.PIPE, text=True)
    if p.stdout.readline().strip() != "ready":
        raise RuntimeError("server did not start")
    return p


async def test_unary(url):
    async with await RpcClient.connect(url) as rpc:
        check("librpc" in rpc.server_name, "server name")
        c = calc.CalcClient(rpc)
        check(await c.add(6, 7) == 13, "add")
        check(await c.sub(1, 0.5) == 0.5, "sub")
        check(await c.sum([1.0, 2, 3.5]) == 6.5, "sum")
        await c.ping()
        check(await c.echo("héllo ✓") == "héllo ✓", "echo")
        check(await c.path_length(calc.Point(x=0, y=0), [calc.Point(x=3, y=0), calc.Point(x=3, y=4)]) == 7, "path")
        big = "x" * (900 * 1024)
        check(len(await c.echo(big)) == len(big), "large message")


async def test_errors(url):
    async with await RpcClient.connect(url) as rpc:
        c = calc.CalcClient(rpc)
        await throws(Code.InvalidArgument, c.div(1, 0), "div by zero")
        await throws(Code.Unimplemented, rpc.call(0x12345678, calc.CalcAddRequest.encode(a=1, b=2), calc.CalcAddResponse.decode), "unknown method")
        check(await c.echo("still here") == "still here", "connection survives")


async def test_server_streams(url):
    async with await RpcClient.connect(url) as rpc:
        c = calc.CalcClient(rpc)
        expected = 1
        async for tick in c.count(1, 50):
            check(tick.n == expected and tick.label == f"tick {expected}", f"tick {expected}")
            expected += 1
        check(expected == 51, "50 ticks")
        check([t.n async for t in c.count(5, 1, -2)] == [5, 3, 1], "step -2")
        n = 0  # 300 items of 16 KiB through the server's 64 KiB window, read slowly
        async for tick in c.count(1, 300, 1, 0, 16 * 1024):
            check(len(tick.payload) == 16 * 1024, "payload")
            n += 1
            if n % 60 == 0:
                await asyncio.sleep(0.025)
        check(n == 300, "300 items")
        seen = 0  # leaving early cancels, and the window is usable afterwards
        async with c.count(1, 100000, 1, 2, 8000) as ticks:
            async for _ in ticks:
                seen += 1
                if seen == 5:
                    break
        check(len([t async for t in c.count(1, 200, 1, 0, 16 * 1024)]) == 200, "window not leaked by the abandoned stream")


async def test_client_streams(url):
    async with await RpcClient.connect(url) as rpc:
        c = calc.CalcClient(rpc)
        total = c.total()
        for k in range(1, 101):
            check(await total.send(k), "send")
        check(await total.finish() == 5050, "total")
        check(await c.total().finish() == 0, "empty total")
        up = c.upload("big")
        chunk = bytes(16 * 1024)
        for _ in range(300):
            check(await up.send({"data": chunk}), "chunk")
        check(await up.finish() == 300 * 16 * 1024, "upload bytes")
        running = c.running()  # bidirectional: each answer follows its question
        answers = running.__aiter__()
        total_so_far = 0.0
        for k in range(1, 21):
            await running.send(k)
            total_so_far += k
            check(await answers.__anext__() == total_so_far, f"running {k}")
        running.end()
        try:
            await answers.__anext__()
            check(False, "ends with ours")
        except StopAsyncIteration:
            pass
        chat = c.chat(">> ")  # both directions at once, through the small window
        pad = "x" * 1000

        async def sender():
            for k in range(1000):
                await chat.send({"text": f"message {k}{pad}"})
            chat.end()

        task = asyncio.create_task(sender())
        lines = 0
        async for line in chat:
            check(line.text == f">> message {lines}{pad}", f"line {lines}")
            lines += 1
        await task
        check(lines == 1000, "1000 lines")
        doomed = c.total()  # cancelling an upload
        await doomed.send(1)
        doomed.cancel()
        check(await c.add(1, 2) == 3, "connection survives a cancelled upload")


async def test_deadlines(url):
    async with await RpcClient.connect(url) as rpc:
        c = calc.CalcClient(rpc)
        t0 = time.monotonic()
        await throws(Code.DeadlineExceeded, c.sleep(5000, timeout=0.15), "deadline")
        check(time.monotonic() - t0 < 2, "deadline is prompt")
        task = asyncio.ensure_future(c.sleep(5000))
        await asyncio.sleep(0.05)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        check(await c.sleep(10, timeout=5) == 10, "within the deadline")
        check(await c.add(1, 1) == 2, "connection survives a cancelled task")


async def test_concurrent(url):
    async with await RpcClient.connect(url) as rpc:
        c = calc.CalcClient(rpc)
        gate = asyncio.Semaphore(32)  # the server refuses more than 64 calls in flight on a session

        async def one(i):
            async with gate:
                return await c.add(i, 1) - i

        results = await asyncio.gather(*(one(i) for i in range(200)))
        check(all(r == 1 for r in results), "200 adds")


async def test_auth(url):
    await throws(Code.Unauthenticated, RpcClient.connect(url), "no token")
    await throws(Code.Unauthenticated, RpcClient.connect(url, token="wrong"), "wrong token")
    async with await RpcClient.connect(url, token="sesame") as rpc:
        check(await calc.CalcClient(rpc).echo("in") == "in", "right token")


async def test_lost(url):
    rpc = await RpcClient.connect(url)
    c = calc.CalcClient(rpc)
    pending = asyncio.ensure_future(c.sleep(5000))
    await asyncio.sleep(0.05)
    await rpc.close()
    await throws(Code.Unavailable, pending, "pending call")
    await throws(Code.Unavailable, c.echo("x"), "after close")


def vec(seed):
    return kitchen.Vec(x=1.5 + seed, y=2.5, z=-3.5, tag=[1, 2, 3 + seed], flag=True, big=-5 - seed)


async def test_natural(url):
    async with await RpcClient.connect(url) as rpc:
        n = kitchen.NaturalClient(rpc)
        text = await n.describe(True, -5, 1.5, kitchen.Color.Green, "hi", vec(0), [1, 2, 3], ["a", "b"], [True, False, True],
                                [vec(1), vec(2)], {"n": 7, "s": "x"})
        check(text == "b=1 big=-5 f=1.5 c=5 s=hi v=1.5,2.5,-3.5,3,1,-5 nums=1,2,3 names=a|b flags=101 vecs=2 inner=7:x d=2.5", f"describe: {text}")
        check("d=4.25" in await n.describe(False, 0, 0, kitchen.Color.Red, "", vec(0), [], [], [], [], {}, 4.25), "default replaced")
        check(await n.squares(4) == [1, 4, 9, 16], "squares")
        check(await n.words() == ["alpha", "", "gamma"], "words")
        check(await n.flip(True) is False, "flip")
        check((await n.origin()).big == vec(0).big, "origin struct")
        made = await n.make("widget")
        check(made.n == 6 and made.s == "widget", "table result")
        check(await n.nothing() is None, "void")
        check([line async for line in n.countdown(3)] == ["t-minus 3", "t-minus 2", "t-minus 1"], "countdown")
        check([c async for c in n.colors()] == [kitchen.Color.Red, kitchen.Color.Green, kitchen.Color.Blue], "colors")
        tally = n.tally(4)
        items = [i.n async for i in tally]
        check(items == [1, 2, 3, 4] and tally.trailer.ok and tally.trailer.message == "4 items", "trailer")
        try:
            await n.fail(0x1234)
            check(False, "fail")
        except RpcError as e:
            check(e.code == Code.Aborted and e.detail == 0x1234, "detail")


def reference():
    return dict(
        tag=0, b=True, bt=False, i8=100, u8=7, i16=31000, u16=1, i32=2000000000, u32=3000000000, i64=9007199254740993,
        u64=18446744073709551000, f32=3.25, f64=1e300, color=kitchen.Color.Blue, text="héllo ✓ wörld", vec=vec(0),
        inner=dict(n=42, s="inner"), bytes=bytes([0, 1, 254, 255]), i8s=[-128, 127], ints=[-1, 0, 2147483647],
        longs=[-9223372036854775807, 0, 9223372036854775807], doubles=[0.5, -1e-300], bools=[True, False, True],
        names=["a", "", "ccc"], colors=[kitchen.Color.Red, kitchen.Color.Green, kitchen.Color.Blue],
        inners=[dict(n=1, s="x"), dict(n=2, s="y")], vecs=[vec(1), vec(2)], late=5)


def check_reference(k, who):
    check(k.b and not k.bt and k.i8 == 100 and k.u8 == 7 and k.i16 == 31000 and k.u16 == 1, f"{who}: small scalars")
    check(k.i32 == 2000000000 and k.u32 == 3000000000 and k.i64 == 9007199254740993 and k.u64 == 18446744073709551000, f"{who}: wide scalars")
    check(k.f32 == 3.25 and k.f64 == 1e300 and k.color == kitchen.Color.Blue and k.text == "héllo ✓ wörld" and k.late == 5, f"{who}: float, enum, string")
    check(k.vec == vec(0) and k.inner.n == 42 and k.inner.s == "inner", f"{who}: struct and table")
    check(k.bytes == bytes([0, 1, 254, 255]) and k.i8s == [-128, 127] and k.ints == [-1, 0, 2147483647], f"{who}: scalar vectors")
    check(k.longs == [-9223372036854775807, 0, 9223372036854775807] and k.doubles == [0.5, -1e-300], f"{who}: wide vectors")
    check(k.bools == [True, False, True] and k.names == ["a", "", "ccc"], f"{who}: bool and string vectors")
    check(k.colors == [kitchen.Color.Red, kitchen.Color.Green, kitchen.Color.Blue] and k.inners[1].s == "y", f"{who}: enum and table vectors")
    check(k.vecs == [vec(1), vec(2)], f"{who}: struct vector")


def check_defaults(k, who):
    check(not k.b and k.bt and k.i8 == -3 and k.u8 == 200 and k.i16 == -1000 and k.u16 == 65000, f"{who}: default small scalars")
    check(k.i32 == -123456 and k.u32 == 4000000000 and k.i64 == -9007199254740993 and k.u64 == 2**64 - 1, f"{who}: default wide scalars")
    check(k.f32 == 1.5 and k.f64 == -2.25 and k.color == kitchen.Color.Green and k.text == "" and k.late == 99, f"{who}: default float, enum, string")
    check(k.inner is None and k.bytes == b"" and k.ints == [] and k.names == [] and k.vecs == [], f"{who}: default tables and vectors")


async def test_every_type(url):
    async with await RpcClient.connect(url) as rpc:
        kc = kitchen.KitchensClient(rpc)
        check_reference(await kc.canned({"which": 0}), "C++ -> Python")
        check_defaults(await kc.canned({"which": 1}), "C++ -> Python")
        verdict = await kc.check(reference())
        check(verdict.ok and verdict.message == "", f"reference: {verdict.message}")
        verdict = await kc.check({"tag": 1})
        check(verdict.ok and verdict.message == "", f"defaults: {verdict.message}")
        verdict = await kc.check({"tag": 0, "b": True, "late": 6})
        check(not verdict.ok and "late" in verdict.message, "a wrong value is reported")


async def main():
    binary = sys.argv[1]
    port, auth_port = free_port(), free_port()
    server = start(binary, port, "--window", "65536")
    guarded = start(binary, auth_port, "--token", "sesame")
    url, auth_url = f"ws://127.0.0.1:{port}/rpc", f"ws://127.0.0.1:{auth_port}/rpc"
    try:
        for name, test, target in (
            ("unary calls", test_unary, url), ("errors carry their status", test_errors, url),
            ("server streaming and flow control", test_server_streams, url),
            ("client streaming and bidirectional calls", test_client_streams, url),
            ("deadlines and cancellation", test_deadlines, url), ("concurrent calls on one connection", test_concurrent, url),
            ("authentication", test_auth, auth_url), ("a lost connection fails the calls", test_lost, url),
            ("natural-form methods of every type, trailers, details", test_natural, url),
            ("every field type, both ways", test_every_type, url),
        ):
            global failures
            before = failures
            try:
                await asyncio.wait_for(test(target), 120)
                print(f"{'ok' if failures == before else 'not ok'} - {name}")
            except Exception as e:  # noqa: BLE001
                failures += 1
                print(f"not ok - {name}: {e!r}", file=sys.stderr)
    finally:
        server.kill()
        guarded.kill()
    print(f"{checks} checks, {failures} failed")
    return 0 if failures == 0 else 1


sys.exit(asyncio.run(main()))
