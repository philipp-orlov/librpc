// Copyright (c) 2026 Philipp Orlov

// A C# client for calc_server: the same calls as the C++, Python and TypeScript examples.
//   dotnet run -- [ws://127.0.0.1:8080/rpc] [token]
// The C# for the schema is made with rpcc (gen/calc.bufs.cs):  rpcc --no-cpp --cs -o examples/cs/CalcClient/gen examples/calc.bufs
using Librpc;
using Sample.Calc;

var url = args.Length > 0 ? args[0] : "ws://127.0.0.1:8080/rpc";
await using var rpc = await RpcClient.ConnectAsync(url, new ClientOptions { Token = args.Length > 1 ? args[1] : null });
var calc = new CalcClient(rpc);

Console.WriteLine($"40 + 2 = {await calc.AddAsync(40, 2)}");
Console.WriteLine($"sum = {await calc.SumAsync(new[] { 1.5, 2.5, 3.0, 4.0 })}");
Console.WriteLine($"echo: '{await calc.EchoAsync("hello, rpc")}'");
Console.WriteLine($"path = {await calc.PathLengthAsync(new Point { X = 0, Y = 0 }, new[] { new Point { X = 3, Y = 0 }, new Point { X = 3, Y = 4 } })}");

await foreach (var tick in calc.CountAsync(1, 5))      // a server stream: items arrive as the server makes them
    Console.WriteLine($"  {tick.Label}");

var total = calc.TotalAsync();                          // client streaming: send many items, then ask for the answer
for (int i = 1; i <= 100; i++)
    await total.SendAsync(i);
Console.WriteLine($"total = {await total.FinishAsync()}");

var running = calc.RunningAsync();                      // bidirectional: each answer follows its question
await using (var answers = running.GetAsyncEnumerator())
{
    var line = "running:";
    foreach (double value in new[] { 1.0, 2.0, 3.0, 4.0 })
    {
        await running.SendAsync(value);
        await answers.MoveNextAsync();
        line += $" {answers.Current}";
    }
    Console.WriteLine(line);
    await running.EndAsync();
}

var upload = calc.UploadAsync("blob");                  // a client stream with a parameter in the call itself
var chunk = new ChunkInit { Data = new byte[4096] };
for (int i = 0; i < 16; i++)
    await upload.SendAsync(chunk);
Console.WriteLine($"upload = {await upload.FinishAsync()}");

try
{
    await calc.DivAsync(1, 0, new CallOptions { Timeout = TimeSpan.FromSeconds(5) });
}
catch (RpcException e)
{
    Console.WriteLine($"expected failure: {e.Code}");
}
