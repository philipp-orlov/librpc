// Copyright (c) 2026 Philipp Orlov

// The TypeScript client against the C++ server (tests/interop_server.cpp): every call kind, flow control,
// cancellation, authentication, and a codec check of every field type in both directions.
import assert from 'node:assert/strict';
import test from 'node:test';

import { Code, RpcClient, RpcError } from '../src/index';
import { CalcClient, CalcMethods } from './gen/calc.bufs';
import { CalcAddRequest } from './gen/calc.bufs';
import { Color, Kitchen, KitchenInit, KitchensClient, NaturalClient, Vec } from './gen/kitchen.bufs';

const url = process.env.RPC_URL!;
const authUrl = process.env.RPC_URL_AUTH!;

async function rejectsWith(code: Code, promise: Promise<unknown>): Promise<void> {
  await assert.rejects(promise, (e: unknown) => e instanceof RpcError && e.code === code, `expected ${Code[code]}`);
}

test('unary calls', async () => {
  const rpc = await RpcClient.connect(url);
  try {
    assert.match(rpc.serverName, /librpc/);
    const calc = new CalcClient(rpc);
    assert.equal(await calc.add(6, 7), 13);
    assert.equal(await calc.sub(1, 0.5), 0.5);
    assert.equal(await calc.sum([1, 2, 3.5]), 6.5);
    await calc.ping();
    const text = 'héllo ✓';
    assert.equal(await calc.echo(text), text);
    // a struct and a vector of structs as parameters
    assert.equal(await calc.pathLength({ x: 0, y: 0 }, [{ x: 3, y: 0 }, { x: 3, y: 4 }]), 7);
    // a large message each way
    const big = 'x'.repeat(900 * 1024);
    assert.equal((await calc.echo(big)).length, big.length);
  } finally {
    rpc.close();
  }
});

test('errors carry their status', async () => {
  const rpc = await RpcClient.connect(url);
  try {
    const calc = new CalcClient(rpc);
    await rejectsWith(Code.InvalidArgument, calc.div(1, 0));
    // a method nobody registered, and the wrong request type for a real one
    const request = CalcAddRequest.encode({ a: 1, b: 2 });
    await rejectsWith(Code.Unimplemented, rpc.call(0x12345678, request, CalcAddRequest.decode));
    await rejectsWith(Code.InvalidArgument, rpc.call(CalcMethods.Echo, request, CalcAddRequest.decode));
    // the connection is still good
    assert.equal(await calc.echo('still here'), 'still here');
  } finally {
    rpc.close();
  }
});

test('server streaming', async () => {
  const rpc = await RpcClient.connect(url);
  try {
    const calc = new CalcClient(rpc);
    let expected = 1;
    for await (const tick of calc.count(1, 50)) {
      assert.equal(tick.n, expected);
      assert.equal(tick.label, `tick ${expected}`);
      expected++;
    }
    assert.equal(expected, 51);
    const seen: number[] = [];
    for await (const tick of calc.count(5, 1, -2)) seen.push(tick.n);
    assert.deepEqual(seen, [5, 3, 1]);
    // the schema default of `step` is 1
    const defaults: number[] = [];
    for await (const tick of calc.count(1, 3)) defaults.push(tick.n);
    assert.deepEqual(defaults, [1, 2, 3]);
  } finally {
    rpc.close();
  }
});

test('flow control: a stream far larger than the window, read slowly', async () => {
  const rpc = await RpcClient.connect(url);
  try {
    const calc = new CalcClient(rpc);
    let n = 0;
    for await (const tick of calc.count(1, 300, 1, 0, 16 * 1024)) {
      assert.equal(tick.payload.length, 16 * 1024);
      n++;
      if (n % 60 === 0) await new Promise((r) => setTimeout(r, 25));
    }
    assert.equal(n, 300);
  } finally {
    rpc.close();
  }
});

test('breaking out of a stream cancels it and leaves the window usable', async () => {
  const rpc = await RpcClient.connect(url);
  try {
    const calc = new CalcClient(rpc);
    let seen = 0;
    for await (const tick of calc.count(1, 100000, 1, 2, 8000)) {
      assert.ok(tick.n > 0);
      if (++seen === 5) break;
    }
    // if the dropped items had never been credited, this could not finish
    let n = 0;
    for await (const _ of calc.count(1, 200, 1, 0, 16 * 1024)) n++;
    assert.equal(n, 200);
  } finally {
    rpc.close();
  }
});

test('client streaming and bidirectional calls', async () => {
  const rpc = await RpcClient.connect(url);
  try {
    const calc = new CalcClient(rpc);
    // client streaming: items up, one answer back
    const total = calc.total();
    for (let k = 1; k <= 100; k++) assert.equal(await total.send(k), true);
    assert.equal(await total.finish(), 5050);
    assert.equal(await calc.total().finish(), 0);
    // a file far larger than the window (the server grants 64 KiB at a time)
    const upload = calc.upload('big');
    const chunk = new Uint8Array(16 * 1024).fill(7);
    for (let k = 0; k < 300; k++) assert.equal(await upload.send({ data: chunk }), true);
    assert.equal(await upload.finish(), BigInt(300 * 16 * 1024));
    // bidirectional: each answer follows its question
    const running = calc.running();
    const answers = running[Symbol.asyncIterator]();
    let expected = 0;
    for (let k = 1; k <= 20; k++) {
      await running.send(k);
      expected += k;
      assert.equal((await answers.next()).value, expected);
    }
    running.end();
    assert.equal((await answers.next()).done, true);
    // bidirectional, sender and reader concurrent, both directions through the small window
    const chat = calc.chat('>> ');
    const pad = 'x'.repeat(1000);
    const sender = (async () => {
      for (let k = 0; k < 1000; k++) await chat.send({ text: `message ${k}${pad}` });
      chat.end();
    })();
    let n = 0;
    for await (const line of chat) {
      assert.equal(line.text, `>> message ${n}${pad}`);
      n++;
    }
    await sender;
    assert.equal(n, 1000);
    // cancelling an upload
    const doomed = calc.total();
    await doomed.send(1);
    doomed.cancel();
    assert.equal(await calc.add(1, 2), 3);
  } finally {
    rpc.close();
  }
});

test('deadlines and abort signals', async () => {
  const rpc = await RpcClient.connect(url);
  try {
    const calc = new CalcClient(rpc);
    const start = Date.now();
    await rejectsWith(Code.DeadlineExceeded, calc.sleep(5000, { timeoutMs: 150 }));
    assert.ok(Date.now() - start < 2000);
    const controller = new AbortController();
    const pending = calc.sleep(5000, { signal: controller.signal });
    setTimeout(() => controller.abort(), 50);
    await rejectsWith(Code.Cancelled, pending);
    assert.equal(await calc.sleep(10, { timeoutMs: 5000 }), 10);
  } finally {
    rpc.close();
  }
});

test('concurrent calls on one connection', async () => {
  const rpc = await RpcClient.connect(url);
  try {
    const calc = new CalcClient(rpc);
    const results = await Promise.all(Array.from({ length: 200 }, (_, i) => calc.add(i, 1).then((r) => r - i)));
    assert.ok(results.every((r) => r === 1));
    const streams = await Promise.all(
      [1, 2, 3, 4].map(async (k) => {
        let n = 0;
        for await (const _ of calc.count(1, 20 * k)) n++;
        return n;
      }),
    );
    assert.deepEqual(streams, [20, 40, 60, 80]);
  } finally {
    rpc.close();
  }
});

test('authentication', async () => {
  await rejectsWith(Code.Unauthenticated, RpcClient.connect(authUrl));
  await rejectsWith(Code.Unauthenticated, RpcClient.connect(authUrl, { token: 'wrong' }));
  const rpc = await RpcClient.connect(authUrl, { token: 'sesame' });
  try {
    assert.equal(await new CalcClient(rpc).echo('in'), 'in');
  } finally {
    rpc.close();
  }
});

test('a lost connection fails the calls', async () => {
  const rpc = await RpcClient.connect(url);
  const calc = new CalcClient(rpc);
  const pending = calc.sleep(5000);
  setTimeout(() => rpc.close(), 50);
  await rejectsWith(Code.Unavailable, pending);
  await rejectsWith(Code.Unavailable, calc.echo('x'));
});

test('natural-form methods of every type', async () => {
  const rpc = await RpcClient.connect(url);
  try {
    const natural = new NaturalClient(rpc);
    const vec = referenceVec(0);
    const text = await natural.describe(true, -5n, 1.5, Color.Green, 'hi', vec, [1, 2, 3], ['a', 'b'], [true, false, true], [referenceVec(1), referenceVec(2)], { n: 7, s: 'x' });
    assert.equal(text, 'b=1 big=-5 f=1.5 c=5 s=hi v=1.5,2.5,-3.5,3,1,-5 nums=1,2,3 names=a|b flags=101 vecs=2 inner=7:x d=2.5');
    assert.match(await natural.describe(false, 0n, 0, Color.Red, '', vec, [], [], [], [], { n: 1, s: '' }, 4.25), /d=4.25/);
    assert.deepEqual(await natural.squares(4), [1n, 4n, 9n, 16n]);
    assert.deepEqual(await natural.words(), ['alpha', '', 'gamma']);
    assert.equal(await natural.flip(true), false);
    assert.deepEqual(await natural.origin(), referenceVec(0));
    const made = await natural.make('widget');
    assert.equal(made.n, 6);
    assert.equal(made.s, 'widget');
    await natural.nothing();
    const lines: string[] = [];
    for await (const line of natural.countdown(3)) lines.push(line);
    assert.deepEqual(lines, ['t-minus 3', 't-minus 2', 't-minus 1']);
    const colors: Color[] = [];
    for await (const c of natural.colors()) colors.push(c);
    assert.deepEqual(colors, [Color.Red, Color.Green, Color.Blue]);
    // a stream with a trailer
    const tally = natural.tally(4);
    let items = 0;
    for await (const item of tally) items += item.n === items + 1 ? 1 : 0;
    assert.equal(items, 4);
    assert.equal(tally.trailer?.ok, true);
    assert.equal(tally.trailer?.message, '4 items');
    // an application detail
    await assert.rejects(natural.fail(0x1234), (e: unknown) => e instanceof RpcError && e.code === Code.Aborted && e.detail === 0x1234);
  } finally {
    rpc.close();
  }
});

// --- every field type ---------------------------------------------------------------------------------------

function referenceVec(seed: number): Vec {
  return { x: 1.5 + seed, y: 2.5, z: -3.5, tag: [1, 2, 3 + seed], flag: true, big: BigInt(-5 - seed) };
}

const reference: KitchenInit = {
  tag: 0,
  b: true,
  bt: false,
  i8: 100,
  u8: 7,
  i16: 31000,
  u16: 1,
  i32: 2000000000,
  u32: 3000000000,
  i64: 9007199254740993n,
  u64: 18446744073709551000n,
  f32: 3.25,
  f64: 1e300,
  color: Color.Blue,
  text: 'héllo ✓ wörld',
  vec: referenceVec(0),
  inner: { n: 42, s: 'inner' },
  bytes: new Uint8Array([0, 1, 254, 255]),
  i8s: [-128, 127],
  ints: [-1, 0, 2147483647],
  longs: [-9223372036854775807n, 0n, 9223372036854775807n],
  doubles: [0.5, -1e-300],
  bools: [true, false, true],
  names: ['a', '', 'ccc'],
  colors: [Color.Red, Color.Green, Color.Blue],
  inners: [
    { n: 1, s: 'x' },
    { n: 2, s: 'y' },
  ],
  vecs: [referenceVec(1), referenceVec(2)],
  late: 5,
};

function checkReference(k: Kitchen): void {
  assert.equal(k.b, true);
  assert.equal(k.bt, false);
  assert.equal(k.i8, 100);
  assert.equal(k.u8, 7);
  assert.equal(k.i16, 31000);
  assert.equal(k.u16, 1);
  assert.equal(k.i32, 2000000000);
  assert.equal(k.u32, 3000000000);
  assert.equal(k.i64, 9007199254740993n);
  assert.equal(k.u64, 18446744073709551000n);
  assert.equal(k.f32, 3.25);
  assert.equal(k.f64, 1e300);
  assert.equal(k.color, Color.Blue);
  assert.equal(k.text, 'héllo ✓ wörld');
  assert.deepEqual(k.vec, referenceVec(0));
  assert.equal(k.inner?.n, 42);
  assert.equal(k.inner?.s, 'inner');
  assert.deepEqual([...k.bytes], [0, 1, 254, 255]);
  assert.deepEqual(k.i8s, [-128, 127]);
  assert.deepEqual(k.ints, [-1, 0, 2147483647]);
  assert.deepEqual(k.longs, [-9223372036854775807n, 0n, 9223372036854775807n]);
  assert.deepEqual(k.doubles, [0.5, -1e-300]);
  assert.deepEqual(k.bools, [true, false, true]);
  assert.deepEqual(k.names, ['a', '', 'ccc']);
  assert.deepEqual(k.colors, [Color.Red, Color.Green, Color.Blue]);
  assert.deepEqual(k.inners.map((i) => [i.n, i.s]), [[1, 'x'], [2, 'y']]);
  assert.deepEqual(k.vecs, [referenceVec(1), referenceVec(2)]);
  assert.equal(k.late, 5);
}

function checkDefaults(k: Kitchen): void {
  assert.equal(k.b, false);
  assert.equal(k.bt, true);
  assert.equal(k.i8, -3);
  assert.equal(k.u8, 200);
  assert.equal(k.i16, -1000);
  assert.equal(k.u16, 65000);
  assert.equal(k.i32, -123456);
  assert.equal(k.u32, 4000000000);
  assert.equal(k.i64, -9007199254740993n);
  assert.equal(k.u64, 18446744073709551615n);
  assert.equal(k.f32, 1.5);
  assert.equal(k.f64, -2.25);
  assert.equal(k.color, Color.Green);
  assert.equal(k.text, '');
  assert.equal(k.inner, null);
  assert.equal(k.vec.x, 0);
  assert.equal(k.bytes.length + k.ints.length + k.names.length + k.vecs.length + k.inners.length, 0);
  assert.equal(k.late, 99);
}

test('every field type: built by C++, read here', async () => {
  const rpc = await RpcClient.connect(url);
  try {
    const kitchens = new KitchensClient(rpc);
    checkReference(await kitchens.canned({ which: 0 }));
    checkDefaults(await kitchens.canned({ which: 1 }));
  } finally {
    rpc.close();
  }
});

test('every field type: built here, checked by C++', async () => {
  const rpc = await RpcClient.connect(url);
  try {
    const kitchens = new KitchensClient(rpc);
    const verdict = await kitchens.check(reference);
    assert.equal(verdict.message, '');
    assert.equal(verdict.ok, true);
    const defaults = await kitchens.check({ tag: 1 });
    assert.equal(defaults.message, '');
    assert.equal(defaults.ok, true);
    // the check can fail: one wrong value is reported
    const wrong = await kitchens.check({ ...reference, late: 6 });
    assert.equal(wrong.ok, false);
    assert.match(wrong.message, /late/);
    // and a round trip through our own codec
    checkReference(Kitchen.decode(Kitchen.encode(reference)));
    checkDefaults(Kitchen.decode(Kitchen.encode({ tag: 1 })));
  } finally {
    rpc.close();
  }
});
