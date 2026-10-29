// Runs the TypeScript tests against the C++ interop server:
//   node run.mjs <interop_server> <rpcc> <work dir>
// 1. rpcc writes the TypeScript for examples/calc.bufs and tests/kitchen.bufs into tests/gen,
// 2. esbuild bundles interop.test.ts, 3. node --test runs it against freshly started servers.
import { spawn, spawnSync } from 'node:child_process';
import { mkdirSync } from 'node:fs';
import net from 'node:net';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const [server, rpcc, work] = process.argv.slice(2);
const here = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(here, '../..');
const gen = path.join(here, 'gen');
mkdirSync(gen, { recursive: true });
mkdirSync(work, { recursive: true });

function run(command, args) {
  const result = spawnSync(command, args, { stdio: 'inherit' });
  if (result.status !== 0) {
    console.error(`${command} failed`);
    process.exit(1);
  }
}

for (const schema of ['examples/calc.bufs', 'tests/kitchen.bufs']) {
  run(rpcc, ['-o', gen, '--no-cpp', '--ts', '--ts-runtime', '../../src/index', path.join(root, schema)]);
}
const bundle = path.join(work, 'interop.test.mjs');
run(path.join(root, 'ts/node_modules/.bin/esbuild'), [
  path.join(here, 'interop.test.ts'),
  '--bundle',
  '--platform=node',
  '--format=esm',
  `--outfile=${bundle}`,
  '--log-level=warning',
]);

function freePort() {
  return new Promise((resolve) => {
    const s = net.createServer();
    s.listen(0, '127.0.0.1', () => {
      const { port } = s.address();
      s.close(() => resolve(port));
    });
  });
}

function start(args) {
  return new Promise(async (resolve, reject) => {
    const port = await freePort();
    const child = spawn(server, [String(port), ...args], { stdio: ['ignore', 'pipe', 'inherit'] });
    child.once('error', reject);
    child.stdout.on('data', (chunk) => {
      if (String(chunk).includes('ready')) resolve({ child, port });
    });
    child.once('exit', (code) => reject(new Error(`server exited early (${code})`)));
  });
}

// Two servers: open, and one that wants a token; a small window makes flow control matter.
const open = await start(['--window', String(64 * 1024)]);
const guarded = await start(['--token', 'sesame']);
const tests = spawn(process.execPath, ['--test', bundle], {
  stdio: 'inherit',
  env: { ...process.env, RPC_URL: `ws://127.0.0.1:${open.port}/rpc`, RPC_URL_AUTH: `ws://127.0.0.1:${guarded.port}/rpc` },
});
const status = await new Promise((resolve) => tests.once('exit', resolve));
for (const { child } of [open, guarded]) {
  child.removeAllListeners('exit');
  child.kill('SIGTERM');
}
process.exit(status ?? 1);
