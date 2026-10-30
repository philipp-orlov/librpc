// Generates the TypeScript for calc.bufs with rpcc and bundles the two TypeScript examples into dist/:
//   node build.mjs <rpcc>      then:  calc_server --web examples/ts/dist   and open http://127.0.0.1:8080/
import { spawnSync } from 'node:child_process';
import { copyFileSync, mkdirSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(here, '../..');
const rpcc = process.argv[2] ?? 'rpcc';
const run = (command, args) => {
  const result = spawnSync(command, args, { stdio: 'inherit' });
  if (result.status !== 0) process.exit(result.status ?? 1);
};
mkdirSync(path.join(here, 'gen'), { recursive: true });
run(rpcc, ['-o', path.join(here, 'gen'), '--no-cpp', '--ts', '--ts-runtime', '../../../ts/src/index', path.join(root, 'examples/calc.bufs')]);
const esbuild = path.join(root, 'ts/node_modules/.bin/esbuild');
run(esbuild, [path.join(here, 'calc_client.ts'), '--bundle', '--platform=node', '--format=esm', `--outfile=${path.join(here, 'dist/calc_client.mjs')}`, '--log-level=warning']);
run(esbuild, [path.join(here, 'browser.ts'), '--bundle', '--format=esm', '--target=es2022', `--outfile=${path.join(here, 'dist/bundle.js')}`, '--log-level=warning']);
copyFileSync(path.join(here, 'index.html'), path.join(here, 'dist/index.html'));
console.log('built', path.join(here, 'dist'));
