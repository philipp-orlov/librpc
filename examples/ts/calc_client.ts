// Copyright (c) 2026 Philipp Orlov

// A Node client for calc_server: the same calls as the C++, C# and Python examples.
//   node dist/calc_client.mjs [ws://127.0.0.1:8080/rpc] [token]
import { RpcClient, RpcError, Code } from '../../ts/src/index';
import { CalcClient } from './gen/calc.bufs';

const url = process.argv[2] ?? 'ws://127.0.0.1:8080/rpc';
const rpc = await RpcClient.connect(url, { token: process.argv[3] });
const calc = new CalcClient(rpc);

console.log(`40 + 2 = ${await calc.add(40, 2)}`);
console.log(`sum = ${await calc.sum([1.5, 2.5, 3, 4])}`);
console.log(`echo: '${await calc.echo('hello, rpc')}'`);
console.log(`path = ${await calc.pathLength({ x: 0, y: 0 }, [{ x: 3, y: 0 }, { x: 3, y: 4 }])}`);

for await (const tick of calc.count(1, 5)) console.log(`  ${tick.label}`); // a server stream; break cancels

const total = calc.total(); // client streaming: send many items, then ask for the answer
for (let i = 1; i <= 100; i++) await total.send(i);
console.log(`total = ${await total.finish()}`);

const running = calc.running(); // bidirectional: each answer follows its question
const answers = running[Symbol.asyncIterator]();
let line = 'running:';
for (const value of [1, 2, 3, 4]) {
  await running.send(value);
  line += ` ${(await answers.next()).value}`;
}
console.log(line);
running.end();

const upload = calc.upload('blob'); // a client stream with a parameter in the call itself
for (let i = 0; i < 16; i++) await upload.send({ data: new Uint8Array(4096).fill(7) });
console.log(`upload = ${await upload.finish()}`);

try {
  await calc.div(1, 0, { timeoutMs: 5000 });
} catch (e) {
  if (!(e instanceof RpcError)) throw e;
  console.log(`expected failure: ${Code[e.code]}`);
}
rpc.close();
