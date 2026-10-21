import { DatabaseSync } from 'node:sqlite';
import type { Broker } from './broker.ts';
import type { Timing } from './config.ts';
import { newMessage, type Message } from './messages.ts';

/** Thrown by a handler that cannot act yet ("park it"): redeliver after a short delay. */
export class Defer extends Error {
  readonly delayMs: number;
  constructor(message: string, delayMs: number) {
    super(message);
    this.delayMs = delayMs;
  }
}

/**
 * The consumer-side retry ladder from the failure-handling slide:
 *   blip       -> immediate retry
 *   outage     -> delayed redelivery with backoff
 *   poison     -> dead-letter after the retry budget
 * Success completes (acks) the message only after the handler has finished: at-least-once.
 */
export function consume(
  broker: Broker,
  endpoint: string,
  timing: Timing,
  handler: (msg: Message) => void | Promise<void>,
) {
  broker.consume(endpoint, async (d) => {
    try {
      await handler(d.msg);
      d.complete();
    } catch (err) {
      const reason = err instanceof Error ? err.message : String(err);
      if (err instanceof Defer) return d.abandon(err.delayMs, { refund: true }); // "not yet" is not a failed attempt
      const n = d.deliveryCount;
      if (n >= timing.maxDeliveries) return d.deadLetter(`${reason} (after ${n} deliveries)`);
      if (n <= timing.immediateRetries) return d.abandon(0);
      const backoff = timing.backoffMs;
      d.abandon(backoff[Math.min(n - timing.immediateRetries - 1, backoff.length - 1)]);
    }
  });
}

export function tx<T>(db: DatabaseSync, fn: () => T): T {
  db.exec('BEGIN');
  try {
    const result = fn();
    db.exec('COMMIT');
    return result;
  } catch (err) {
    db.exec('ROLLBACK');
    throw err;
  }
}

/**
 * The transactional outbox. A component writes its state change and the messages it wants to send
 * in ONE local transaction (`add` is called inside the caller's tx). The relay publishes unsent rows
 * afterwards and marks them sent. If the relay dies between publishing and marking, the row goes
 * out twice: fine, because every consumer is idempotent.
 */
export class Outbox {
  private readonly db: DatabaseSync;
  private readonly broker: Broker;
  private readonly faults: { relayCrashOnce: boolean };

  constructor(db: DatabaseSync, broker: Broker, faults: { relayCrashOnce: boolean }) {
    this.db = db;
    this.broker = broker;
    this.faults = faults;
    db.exec(`CREATE TABLE IF NOT EXISTS outbox (
      seq INTEGER PRIMARY KEY AUTOINCREMENT,
      route TEXT NOT NULL,
      message TEXT NOT NULL,
      delay_ms INTEGER NOT NULL DEFAULT 0,
      sent INTEGER NOT NULL DEFAULT 0
    )`);
  }

  add(route: string, msg: Message, delayMs = 0) {
    this.db.prepare('INSERT INTO outbox (route, message, delay_ms) VALUES (?, ?, ?)').run(route, JSON.stringify(msg), delayMs);
  }

  pending(): number {
    return (this.db.prepare('SELECT COUNT(*) AS n FROM outbox WHERE sent = 0').get() as { n: number }).n;
  }

  relay() {
    const rows = this.db.prepare('SELECT seq, route, message, delay_ms FROM outbox WHERE sent = 0 ORDER BY seq').all() as {
      seq: number;
      route: string;
      message: string;
      delay_ms: number;
    }[];
    for (const row of rows) {
      this.broker.route(row.route, JSON.parse(row.message), { delayMs: row.delay_ms });
      if (this.faults.relayCrashOnce) {
        this.faults.relayCrashOnce = false;
        return; // "crashed" after publishing, before marking sent: the next pass publishes it again
      }
      this.db.prepare('UPDATE outbox SET sent = 1 WHERE seq = ?').run(row.seq);
    }
  }
}

export { newMessage };
