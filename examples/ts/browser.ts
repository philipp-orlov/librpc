// Copyright (c) 2026 Philipp Orlov

// The browser side of the Calc example: one WebSocket to the page's own origin.
import { RpcClient } from '../../ts/src/index';
import { CalcClient } from './gen/calc.bufs';

const $ = <T extends HTMLElement>(id: string): T => document.getElementById(id) as T;
const log = (text: string): void => {
  $('log').textContent += text + '\n';
};

const url = `${location.protocol === 'https:' ? 'wss' : 'ws'}://${location.host}/rpc`;
const rpc = await RpcClient.connect(url);
const calc = new CalcClient(rpc);
log(`connected to ${rpc.serverName} (${url})`);

const ops: Record<string, (a: number, b: number) => Promise<number>> = {
  '+': (a, b) => calc.add(a, b),
  '-': (a, b) => calc.sub(a, b),
  '/': (a, b) => calc.div(a, b),
};

$('compute').addEventListener('click', async () => {
  const op = $<HTMLSelectElement>('op').value;
  const a = Number($<HTMLInputElement>('a').value);
  const b = Number($<HTMLInputElement>('b').value);
  try {
    log(`${a} ${op} ${b} = ${await ops[op](a, b)}`);
  } catch (e) {
    log(String(e));
  }
});

$('count').addEventListener('click', async () => {
  for await (const tick of calc.count(1, 10, 1, 200)) log(tick.label);
});
