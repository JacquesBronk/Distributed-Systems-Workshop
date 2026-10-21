import { createServer, type IncomingMessage, type ServerResponse } from 'node:http';
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { createSystem, LIVE, type Faults, type System } from './system.ts';

// Dashboard + control API. Commands from the facilitator go in over HTTP; the dashboard is a *query*
// over the orchestrator's state history and the broker's stats, so it never goes through the bus.

const PORT = Number(process.env.PORT ?? 3000);
const page = fileURLToPath(new URL('../public/index.html', import.meta.url));

let sys: System = createSystem(LIVE).start();
const streams = new Set<ServerResponse>();

function push() {
  const data = `data: ${JSON.stringify(sys.snapshot())}\n\n`;
  for (const res of streams) res.write(data);
}
let pending: NodeJS.Timeout | undefined;
function onChange() {
  // Coalesce bursts of broker activity into at most ~10 pushes a second.
  if (pending) return;
  pending = setTimeout(() => {
    pending = undefined;
    if (streams.size) push();
  }, 100);
}
sys.broker.on('change', onChange);

function reset() {
  sys.stop();
  sys.broker.off('change', onChange);
  sys = createSystem(LIVE).start();
  sys.broker.on('change', onChange);
  push();
}

const FAULT_KEYS: (keyof Faults)[] = ['outboxEnabled', 'repairDown', 'paymentsIdempotent', 'crashNextCommit', 'relayCrashOnce', 'repairDelayMs', 'assessmentDelayMs'];

async function body(req: IncomingMessage): Promise<any> {
  let raw = '';
  for await (const chunk of req) raw += chunk;
  return raw ? JSON.parse(raw) : {};
}

const json = (res: ServerResponse, status: number, value: unknown) => {
  res.writeHead(status, { 'content-type': 'application/json' });
  res.end(JSON.stringify(value));
};

createServer(async (req, res) => {
  try {
    const url = new URL(req.url ?? '/', 'http://localhost');
    const route = `${req.method} ${url.pathname}`;

    if (route === 'GET /') {
      res.writeHead(200, { 'content-type': 'text/html; charset=utf-8' });
      return res.end(await readFile(page));
    }
    if (route === 'GET /favicon.ico') return res.writeHead(204).end();
    if (route === 'GET /api/state') return json(res, 200, sys.snapshot());
    if (route === 'GET /api/events') {
      res.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-cache', connection: 'keep-alive' });
      streams.add(res);
      res.write(`data: ${JSON.stringify(sys.snapshot())}\n\n`);
      req.on('close', () => streams.delete(res));
      return;
    }

    if (req.method !== 'POST') return json(res, 404, { error: 'not found' });
    const b = await body(req);

    if (url.pathname === '/api/claims') {
      const amount = Number(b.amount ?? 2400);
      if (!(amount >= 0)) return json(res, 400, { error: 'amount must be a number' });
      return json(res, 201, { claimId: sys.submit({ ...b, amount }) });
    }
    const cancel = url.pathname.match(/^\/api\/claims\/([\w-]+)\/cancel$/);
    if (cancel) {
      sys.cancel(cancel[1]);
      return json(res, 202, { ok: true });
    }
    if (url.pathname === '/api/faults') {
      if (!FAULT_KEYS.includes(b.name)) return json(res, 400, { error: `unknown fault ${b.name}` });
      Object.assign(sys.faults, { [b.name]: b.value });
      push();
      return json(res, 200, sys.faults);
    }
    if (url.pathname === '/api/inject/poison') {
      sys.injectPoison();
      return json(res, 202, { ok: true });
    }
    if (url.pathname === '/api/inject/duplicate') {
      const allowed: Record<string, [string, string]> = { PaymentSettled: ['orchestrator', 'PaymentSettled'], SettlePayment: ['payments', 'SettlePayment'] };
      const target = allowed[b.type];
      if (!target) return json(res, 400, { error: 'type must be PaymentSettled or SettlePayment' });
      sys.duplicateNext(...target);
      return json(res, 202, { ok: true });
    }
    if (url.pathname === '/api/dlq/replay') return json(res, 200, { ok: sys.broker.replay(b.endpoint, b.id, b.body) });
    if (url.pathname === '/api/dlq/discard') return json(res, 200, { ok: sys.broker.discard(b.endpoint, b.id) });
    if (url.pathname === '/api/reset') {
      reset();
      return json(res, 200, { ok: true });
    }
    json(res, 404, { error: 'not found' });
  } catch (err) {
    json(res, 500, { error: err instanceof Error ? err.message : String(err) });
  }
}).listen(PORT, () => console.log(`Claims demo running at http://localhost:${PORT}`));
