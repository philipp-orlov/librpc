// Copyright (c) 2026 Philipp Orlov

// The C# client against the C++ server (tests/interop_server.cpp): every call kind, flow control, cancellation,
// authentication, and a codec check of every field type in both directions.
//   dotnet run -- <interop_server>
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text;
using Librpc;
using Sample.Calc;
using Test;

int failures = 0, checks = 0;
void Check(bool ok, string what)
{
    checks++;
    if (!ok) { failures++; Console.Error.WriteLine($"  CHECK FAILED: {what}"); }
}
async Task Test(string name, Func<Task> body)
{
    try
    {
        await body();
        Console.WriteLine($"ok - {name}");
    }
    catch (Exception e)
    {
        failures++;
        Console.Error.WriteLine($"not ok - {name}: {e}");
    }
}
async Task Throws(Code code, Func<Task> body, string what)
{
    try { await body(); Check(false, $"{what}: expected {code}, nothing thrown"); }
    catch (RpcException e) { Check(e.Code == code, $"{what}: expected {code}, got {e.Code}"); }
}

int FreePort()
{
    var l = new TcpListener(IPAddress.Loopback, 0);
    l.Start();
    int port = ((IPEndPoint)l.LocalEndpoint).Port;
    l.Stop();
    return port;
}

Process Start(string binary, int port, params string[] args)
{
    var psi = new ProcessStartInfo(binary) { RedirectStandardOutput = true, UseShellExecute = false };
    psi.ArgumentList.Add(port.ToString());
    foreach (var a in args) psi.ArgumentList.Add(a);
    var p = Process.Start(psi)!;
    var line = p.StandardOutput.ReadLine();
    if (line != "ready") throw new Exception("server did not start");
    return p;
}

var binary = args.Length > 0 ? args[0] : throw new ArgumentException("usage: Librpc.Tests <interop_server>");
int port = FreePort(), authPort = FreePort();
using var server = Start(binary, port, "--window", "65536");
using var guarded = Start(binary, authPort, "--token", "sesame");
string url = $"ws://127.0.0.1:{port}/rpc", authUrl = $"ws://127.0.0.1:{authPort}/rpc";

await Test("unary calls", async () =>
{
    await using var rpc = await RpcClient.ConnectAsync(url);
    Check(rpc.ServerName.Contains("librpc"), "server name");
    var calc = new CalcClient(rpc);
    Check(await calc.AddAsync(6, 7) == 13, "add");
    Check(await calc.SubAsync(1, 0.5) == 0.5, "sub");
    Check(await calc.SumAsync(new[] { 1.0, 2, 3.5 }) == 6.5, "sum");
    await calc.PingAsync();
    Check(await calc.EchoAsync("héllo ✓") == "héllo ✓", "echo");
    Check(await calc.PathLengthAsync(new Point { X = 0, Y = 0 }, new[] { new Point { X = 3, Y = 0 }, new Point { X = 3, Y = 4 } }) == 7, "path");
    var big = new string('x', 900 * 1024);
    Check((await calc.EchoAsync(big)).Length == big.Length, "large message");
});

await Test("errors carry their status", async () =>
{
    await using var rpc = await RpcClient.ConnectAsync(url);
    var calc = new CalcClient(rpc);
    await Throws(Code.InvalidArgument, () => calc.DivAsync(1, 0), "div by zero");
    await Throws(Code.Unimplemented, () => rpc.CallAsync(0x12345678, CalcAddRequest.Encode(new CalcAddRequestInit { A = 1, B = 2 }), b => CalcAddRequest.Decode(b)), "unknown method");
    await Throws(Code.InvalidArgument, () => rpc.CallAsync(CalcMethods.Echo, CalcAddRequest.Encode(new CalcAddRequestInit { A = 1 }), b => CalcAddRequest.Decode(b)), "wrong request type");
    Check(await calc.EchoAsync("still here") == "still here", "connection survives");
});

await Test("server streaming and flow control", async () =>
{
    await using var rpc = await RpcClient.ConnectAsync(url);
    var calc = new CalcClient(rpc);
    int expected = 1;
    await foreach (var tick in calc.CountAsync(1, 50))
    {
        Check(tick.N == expected && tick.Label == $"tick {expected}", $"tick {expected}");
        expected++;
    }
    Check(expected == 51, "50 ticks");
    var down = new List<int>();
    await foreach (var tick in calc.CountAsync(5, 1, -2)) down.Add(tick.N);
    Check(down.SequenceEqual(new[] { 5, 3, 1 }), "step -2");
    // 300 items of 16 KiB through the server's 64 KiB window, read slowly
    int n = 0;
    await foreach (var tick in calc.CountAsync(1, 300, 1, 0, 16 * 1024))
    {
        Check(tick.Payload.Length == 16 * 1024, "payload");
        if (++n % 60 == 0) await Task.Delay(25);
    }
    Check(n == 300, "300 items");
    // breaking out cancels, and the window is usable afterwards
    int seen = 0;
    await foreach (var tick in calc.CountAsync(1, 100000, 1, 2, 8000)) if (++seen == 5) break;
    int again = 0;
    await foreach (var tick in calc.CountAsync(1, 200, 1, 0, 16 * 1024)) again++;
    Check(again == 200, "window not leaked by the abandoned stream");
});

await Test("client streaming and bidirectional calls", async () =>
{
    await using var rpc = await RpcClient.ConnectAsync(url);
    var calc = new CalcClient(rpc);
    var total = calc.TotalAsync();
    for (int k = 1; k <= 100; k++) Check(await total.SendAsync(k), "send");
    Check(await total.FinishAsync() == 5050, "total");
    Check(await calc.TotalAsync().FinishAsync() == 0, "empty total");
    var upload = calc.UploadAsync("big");
    var chunk = new ChunkInit { Data = new byte[16 * 1024] };
    for (int k = 0; k < 300; k++) Check(await upload.SendAsync(chunk), "chunk");
    Check(await upload.FinishAsync() == 300UL * 16 * 1024, "upload bytes");
    // bidirectional: each answer follows its question
    var running = calc.RunningAsync();
    await using (var answers = running.GetAsyncEnumerator())
    {
        double sum = 0;
        for (int k = 1; k <= 20; k++)
        {
            await running.SendAsync(k);
            sum += k;
            Check(await answers.MoveNextAsync() && answers.Current == sum, $"running {k}");
        }
        await running.EndAsync();
        Check(!await answers.MoveNextAsync(), "ends with ours");
    }
    // sender and reader concurrent, both directions through the small window
    var chat = calc.ChatAsync(">> ");
    var pad = new string('x', 1000);
    var sender = Task.Run(async () =>
    {
        for (int k = 0; k < 1000; k++) await chat.SendAsync(new LineInit { Text = $"message {k}{pad}" });
        await chat.EndAsync();
    });
    int lines = 0;
    await foreach (var line in chat)
    {
        Check(line.Text == $">> message {lines}{pad}", $"line {lines}");
        lines++;
    }
    await sender;
    Check(lines == 1000, "1000 lines");
    // cancelling an upload
    var doomed = calc.TotalAsync();
    await doomed.SendAsync(1);
    doomed.Cancel();
    Check(await calc.AddAsync(1, 2) == 3, "connection survives a cancelled upload");
});

await Test("deadlines and cancellation tokens", async () =>
{
    await using var rpc = await RpcClient.ConnectAsync(url);
    var calc = new CalcClient(rpc);
    var sw = Stopwatch.StartNew();
    await Throws(Code.DeadlineExceeded, () => calc.SleepAsync(5000, new CallOptions { Timeout = TimeSpan.FromMilliseconds(150) }), "deadline");
    Check(sw.ElapsedMilliseconds < 2000, "deadline is prompt");
    using var cts = new CancellationTokenSource(50);
    await Throws(Code.Cancelled, () => calc.SleepAsync(5000, new CallOptions { CancellationToken = cts.Token }), "token");
    Check(await calc.SleepAsync(10, new CallOptions { Timeout = TimeSpan.FromSeconds(5) }) == 10, "within the deadline");
});

await Test("concurrent calls on one connection", async () =>
{
    await using var rpc = await RpcClient.ConnectAsync(url);
    var calc = new CalcClient(rpc);
    // 200 calls, at most 32 at a time: the server refuses more than 64 in flight on a session (ResourceExhausted).
    using var gate = new SemaphoreSlim(32);
    var results = await Task.WhenAll(Enumerable.Range(0, 200).Select(async i =>
    {
        await gate.WaitAsync();
        try { return await calc.AddAsync(i, 1) - i; }
        finally { gate.Release(); }
    }));
    Check(results.All(r => r == 1), "200 adds");
});

await Test("authentication", async () =>
{
    await Throws(Code.Unauthenticated, async () => { await RpcClient.ConnectAsync(authUrl); }, "no token");
    await Throws(Code.Unauthenticated, async () => { await RpcClient.ConnectAsync(authUrl, new ClientOptions { Token = "wrong" }); }, "wrong token");
    await using var rpc = await RpcClient.ConnectAsync(authUrl, new ClientOptions { Token = "sesame" });
    Check(await new CalcClient(rpc).EchoAsync("in") == "in", "right token");
});

await Test("a lost connection fails the calls", async () =>
{
    var rpc = await RpcClient.ConnectAsync(url);
    var calc = new CalcClient(rpc);
    var pending = calc.SleepAsync(5000);
    _ = Task.Run(async () => { await Task.Delay(50); await rpc.DisposeAsync(); });
    await Throws(Code.Unavailable, () => pending, "pending call");
    await Throws(Code.Unavailable, () => calc.EchoAsync("x"), "after close");
});

await Test("natural-form methods of every type, trailers, details", async () =>
{
    await using var rpc = await RpcClient.ConnectAsync(url);
    var natural = new NaturalClient(rpc);
    var vec = ReferenceVec(0);
    var text = await natural.DescribeAsync(true, -5, 1.5f, Color.Green, "hi", vec, new[] { 1, 2, 3 }, new[] { "a", "b" }, new[] { true, false, true },
        new[] { ReferenceVec(1), ReferenceVec(2) }, new InnerInit { N = 7, S = "x" });
    Check(text == "b=1 big=-5 f=1.5 c=5 s=hi v=1.5,2.5,-3.5,3,1,-5 nums=1,2,3 names=a|b flags=101 vecs=2 inner=7:x d=2.5", $"describe: {text}");
    Check((await natural.DescribeAsync(false, 0, 0, Color.Red, "", vec, Array.Empty<int>(), Array.Empty<string>(), Array.Empty<bool>(), Array.Empty<Vec>(), new InnerInit(), 4.25)).Contains("d=4.25"), "default replaced");
    Check((await natural.SquaresAsync(4)).SequenceEqual(new long[] { 1, 4, 9, 16 }), "squares");
    Check((await natural.WordsAsync()).SequenceEqual(new[] { "alpha", "", "gamma" }), "words");
    Check(await natural.FlipAsync(true) == false, "flip");
    Check((await natural.OriginAsync()).Big == ReferenceVec(0).Big, "origin struct");
    var made = await natural.MakeAsync("widget");
    Check(made.N == 6 && made.S == "widget", "table result");
    await natural.NothingAsync();
    var lines = new List<string>();
    await foreach (var line in natural.CountdownAsync(3)) lines.Add(line);
    Check(lines.SequenceEqual(new[] { "t-minus 3", "t-minus 2", "t-minus 1" }), "countdown");
    var colors = new List<Color>();
    await foreach (var c in natural.ColorsAsync()) colors.Add(c);
    Check(colors.SequenceEqual(new[] { Color.Red, Color.Green, Color.Blue }), "colors");
    var tally = natural.TallyAsync(4);
    int items = 0;
    await foreach (var item in tally) items += item.N == items + 1 ? 1 : 0;
    Check(items == 4 && tally.Trailer.Ok && tally.Trailer.Message == "4 items", "trailer");
    try { await natural.FailAsync(0x1234); Check(false, "fail"); }
    catch (RpcException e) { Check(e.Code == Code.Aborted && e.Detail == 0x1234, "detail"); }
});

static Vec ReferenceVec(int seed) => new Vec { X = 1.5f + seed, Y = 2.5f, Z = -3.5f, Tag = new byte[] { 1, 2, (byte)(3 + seed) }, Flag = true, Big = -5 - seed };

static KitchenInit Reference() => new KitchenInit
{
    Tag = 0, B = true, Bt = false, I8 = 100, U8 = 7, I16 = 31000, U16 = 1, I32 = 2000000000, U32 = 3000000000, I64 = 9007199254740993, U64 = 18446744073709551000UL,
    F32 = 3.25f, F64 = 1e300, Color = Color.Blue, Text = "héllo ✓ wörld", Vec = ReferenceVec(0), Inner = new InnerInit { N = 42, S = "inner" },
    Bytes = new byte[] { 0, 1, 254, 255 }, I8s = new sbyte[] { -128, 127 }, Ints = new[] { -1, 0, 2147483647 },
    Longs = new[] { -9223372036854775807, 0, 9223372036854775807 }, Doubles = new[] { 0.5, -1e-300 }, Bools = new[] { true, false, true },
    Names = new[] { "a", "", "ccc" }, Colors = new[] { Color.Red, Color.Green, Color.Blue },
    Inners = new[] { new InnerInit { N = 1, S = "x" }, new InnerInit { N = 2, S = "y" } }, Vecs = new[] { ReferenceVec(1), ReferenceVec(2) }, Late = 5,
};

void CheckReference(Kitchen k)
{
    Check(k.B && !k.Bt && k.I8 == 100 && k.U8 == 7 && k.I16 == 31000 && k.U16 == 1, "small scalars");
    Check(k.I32 == 2000000000 && k.U32 == 3000000000 && k.I64 == 9007199254740993 && k.U64 == 18446744073709551000UL, "wide scalars");
    Check(k.F32 == 3.25f && k.F64 == 1e300 && k.Color == Color.Blue && k.Text == "héllo ✓ wörld" && k.Late == 5, "float, enum, string");
    Check(k.Vec.X == 1.5f && k.Vec.Tag.SequenceEqual(new byte[] { 1, 2, 3 }) && k.Vec.Flag && k.Vec.Big == -5, "struct");
    Check(k.Inner is { N: 42 } inner && inner.S == "inner", "table");
    Check(k.Bytes.SequenceEqual(new byte[] { 0, 1, 254, 255 }) && k.I8s.SequenceEqual(new sbyte[] { -128, 127 }) && k.Ints.SequenceEqual(new[] { -1, 0, 2147483647 }), "scalar vectors");
    Check(k.Longs.SequenceEqual(new[] { -9223372036854775807, 0, 9223372036854775807 }) && k.Doubles.SequenceEqual(new[] { 0.5, -1e-300 }), "wide vectors");
    Check(k.Bools.SequenceEqual(new[] { true, false, true }) && k.Names.SequenceEqual(new[] { "a", "", "ccc" }), "bool and string vectors");
    Check(k.Colors.SequenceEqual(new[] { Color.Red, Color.Green, Color.Blue }) && k.Inners.Length == 2 && k.Inners[1].S == "y", "enum and table vectors");
    Check(k.Vecs.Length == 2 && k.Vecs[1].Tag[2] == 5 && k.Vecs[1].Big == -7, "struct vector");
}

void CheckDefaults(Kitchen k)
{
    Check(!k.B && k.Bt && k.I8 == -3 && k.U8 == 200 && k.I16 == -1000 && k.U16 == 65000, "default small scalars");
    Check(k.I32 == -123456 && k.U32 == 4000000000 && k.I64 == -9007199254740993 && k.U64 == ulong.MaxValue, "default wide scalars");
    Check(k.F32 == 1.5f && k.F64 == -2.25 && k.Color == Color.Green && k.Text == "" && k.Late == 99, "default float, enum, string");
    Check(k.Inner == null && k.Vec.X == 0 && k.Bytes.Length == 0 && k.Ints.Length == 0 && k.Names.Length == 0 && k.Vecs.Length == 0, "defaults of tables and vectors");
}

await Test("every field type: built by C++, read here", async () =>
{
    await using var rpc = await RpcClient.ConnectAsync(url);
    var kitchens = new KitchensClient(rpc);
    CheckReference(await kitchens.CannedAsync(new PickInit { Which = 0 }));
    CheckDefaults(await kitchens.CannedAsync(new PickInit { Which = 1 }));
});

await Test("every field type: built here, checked by C++", async () =>
{
    await using var rpc = await RpcClient.ConnectAsync(url);
    var kitchens = new KitchensClient(rpc);
    var verdict = await kitchens.CheckAsync(Reference());
    Check(verdict.Ok && verdict.Message == "", $"reference: {verdict.Message}");
    var defaults = await kitchens.CheckAsync(new KitchenInit { Tag = 1 });
    Check(defaults.Ok && defaults.Message == "", $"defaults: {defaults.Message}");
    var wrong = await kitchens.CheckAsync(new KitchenInit { Tag = 0, B = true, Late = 6 });
    Check(!wrong.Ok && wrong.Message.Contains("late"), "a wrong value is reported");
    CheckReference(Kitchen.Decode(Kitchen.Encode(Reference())));
    CheckDefaults(Kitchen.Decode(Kitchen.Encode(new KitchenInit { Tag = 1 })));
});

server.Kill();
guarded.Kill();
Console.WriteLine($"{checks} checks, {failures} failed");
return failures == 0 ? 0 : 1;
