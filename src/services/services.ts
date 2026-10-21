import { DatabaseSync } from 'node:sqlite';
import type { Broker } from '../broker.ts';
import type { Timing } from '../config.ts';
import { CLAIM_EVENTS, newMessage, rand, sleep, type Message } from '../messages.ts';
import { Outbox, tx } from '../runtime.ts';

export interface ServiceFaults {
  repairDown: boolean; // the repair provider is unreachable
  repairDelayMs: number; // slows BookRepair, so a cancellation can overtake it
  assessmentDelayMs: number; // slows assessment, so the fraud result arrives first
  paymentsIdempotent: boolean; // off = no inbox, no natural key: duplicates pay twice
  relayCrashOnce: boolean;
}

const need = (cond: unknown, what: string): void => {
  if (!cond) throw new Error(`invalid message: ${what}`);
};

const reply = (broker: Broker, cause: Message, type: string, body: Record<string, unknown>) =>
  broker.publish(CLAIM_EVENTS, newMessage('event', type, { claimId: cause.correlationId, ...body }, cause.correlationId, cause.id));

// ---------------------------------------------------------------------------------------------
// Stateless services publish their outcome straight to the topic. Recomputing on redelivery is harmless.
// ---------------------------------------------------------------------------------------------

export function assessmentHandler(broker: Broker, t: Timing, f: ServiceFaults) {
  return async (msg: Message) => {
    need(msg.type === 'AssessDamage' && typeof msg.body.claimId === 'string', 'AssessDamage');
    await sleep(rand(...t.latency) + f.assessmentDelayMs);
    reply(broker, msg, msg.body.outcome === 'fail' ? 'AssessmentFailed' : 'AssessmentPassed', {});
  };
}

export function fraudHandler(broker: Broker, t: Timing) {
  return async (msg: Message) => {
    need(msg.type === 'CheckFraud' && typeof msg.body.claimId === 'string', 'CheckFraud');
    await sleep(rand(...t.latency));
    if (msg.body.outcome === 'none') return reply(broker, msg, 'FraudCleared', {});
    reply(broker, msg, 'FraudSuspected', {});
    // The analyst's decision arrives later, on its own: nobody is waiting on this call.
    const verdict = msg.body.outcome === 'review-confirm' ? 'FraudConfirmed' : 'FraudCleared';
    setTimeout(() => reply(broker, msg, verdict, {}), t.fraudReviewMs);
  };
}

// ---------------------------------------------------------------------------------------------
// Stateful services own a database, so they use the outbox for what they publish.
// ---------------------------------------------------------------------------------------------

export class RepairService {
  readonly db = new DatabaseSync(':memory:');
  readonly outbox: Outbox;
  private readonly t: Timing;
  private readonly f: ServiceFaults;

  constructor(broker: Broker, t: Timing, f: ServiceFaults) {
    this.t = t;
    this.f = f;
    this.db.exec('CREATE TABLE bookings (claim_id TEXT PRIMARY KEY, status TEXT NOT NULL)');
    this.outbox = new Outbox(this.db, broker, f);
  }

  private emit(cause: Message, type: string, extra: Record<string, unknown> = {}) {
    this.outbox.add(`topic:${CLAIM_EVENTS}`, newMessage('event', type, { claimId: cause.correlationId, ...extra }, cause.correlationId, cause.id));
  }

  book = async (msg: Message) => {
    need(msg.type === 'BookRepair' && typeof msg.body.claimId === 'string', 'BookRepair');
    await sleep(rand(...this.t.latency) + this.f.repairDelayMs);
    if (this.f.repairDown) throw new Error('repair provider unavailable');
    const claimId = msg.body.claimId;
    tx(this.db, () => {
      // Naturally idempotent: one booking per claim, however many times the command arrives.
      this.db.prepare("INSERT INTO bookings (claim_id, status) VALUES (?, 'booked') ON CONFLICT(claim_id) DO NOTHING").run(claimId);
      // The event carries what a garage needs to do the job. Dispatching it is the outbound connector's business;
      // the completion comes back later, through the inbound connector, in our vocabulary.
      this.emit(msg, 'RepairBooked', { description: msg.body.description, customer: msg.body.customer });
    });
  };

  /** The saga's compensating action: a business operation that can be retried safely. */
  cancel = async (msg: Message) => {
    need(msg.type === 'CancelRepairBooking' && typeof msg.body.claimId === 'string', 'CancelRepairBooking');
    await sleep(rand(...this.t.latency));
    tx(this.db, () => {
      this.db.prepare("INSERT INTO bookings (claim_id, status) VALUES (?, 'cancelled') ON CONFLICT(claim_id) DO UPDATE SET status = 'cancelled'").run(msg.body.claimId);
      this.emit(msg, 'RepairBookingCancelled');
    });
  };

  bookings() {
    return this.db.prepare('SELECT claim_id AS claimId, status FROM bookings ORDER BY rowid DESC').all() as { claimId: string; status: string }[];
  }
}

export class PaymentsService {
  readonly db = new DatabaseSync(':memory:');
  readonly outbox: Outbox;
  private readonly t: Timing;
  private readonly f: ServiceFaults;

  constructor(broker: Broker, t: Timing, f: ServiceFaults) {
    this.t = t;
    this.f = f;
    this.db.exec(`
      CREATE TABLE inbox (message_id TEXT PRIMARY KEY);
      CREATE TABLE ledger (seq INTEGER PRIMARY KEY AUTOINCREMENT, claim_id TEXT NOT NULL, amount REAL NOT NULL, message_id TEXT NOT NULL, at INTEGER NOT NULL);
    `);
    this.outbox = new Outbox(this.db, broker, f);
  }

  settle = async (msg: Message) => {
    // A malformed message is a poison message: it will never succeed, so retries only delay the dead-letter queue.
    need(msg.type === 'SettlePayment', 'expected SettlePayment');
    need(typeof msg.body.claimId === 'string' && msg.body.claimId.length > 0, 'claimId is required');
    need(typeof msg.body.amount === 'number' && msg.body.amount > 0, 'amount must be a positive number');
    await sleep(rand(...this.t.latency));
    const { claimId, amount } = msg.body as { claimId: string; amount: number };

    tx(this.db, () => {
      if (this.f.paymentsIdempotent) {
        // Inbox: skip a MessageId we already handled (redelivery after a lost ack).
        const seen = this.db.prepare('INSERT OR IGNORE INTO inbox (message_id) VALUES (?)').run(msg.id);
        if (seen.changes === 0) return;
        // Natural key: a timeout resend is a *new* message for the same claim, so the MessageId alone isn't enough.
        const paid = this.db.prepare('SELECT 1 FROM ledger WHERE claim_id = ?').get(claimId);
        if (paid) return;
      }
      this.db.prepare('INSERT INTO ledger (claim_id, amount, message_id, at) VALUES (?, ?, ?, ?)').run(claimId, amount, msg.id, Date.now());
      this.outbox.add(`topic:${CLAIM_EVENTS}`, newMessage('event', 'PaymentSettled', { claimId }, msg.correlationId, msg.id));
    });
  };

  ledger() {
    return this.db.prepare('SELECT claim_id AS claimId, amount, at FROM ledger ORDER BY seq DESC').all() as { claimId: string; amount: number; at: number }[];
  }
}

// ---------------------------------------------------------------------------------------------
// Choreographed edges: they react to events and decide for themselves. The orchestrator never commands them.
// ---------------------------------------------------------------------------------------------

export class Notifications {
  readonly sent: { at: number; claimId: string; text: string }[] = [];
  private seen = new Set<string>(); // an inbox of one: no duplicate emails on redelivery

  handle = (msg: Message) => {
    if (this.seen.has(msg.id)) return;
    this.seen.add(msg.id);
    const text: Record<string, string> = {
      ClaimApproved: 'Good news: your claim is approved and a repairer is being booked.',
      ClaimRejected: `We could not accept your claim (${msg.body.reason ?? 'see details'}).`,
      ClaimClosed: 'Your repair is complete and the claim is settled. Thank you.',
      ClaimCancelled: 'Your claim has been cancelled, as you asked.',
      ClaimEscalated: `Internal alert: claim stuck in ${msg.body.state}, needs a human.`,
    };
    if (text[msg.type]) this.sent.push({ at: Date.now(), claimId: msg.correlationId, text: text[msg.type] });
  };
}

export class Audit {
  readonly entries: { at: number; claimId: string; type: string }[] = [];
  handle = (msg: Message) => {
    this.entries.push({ at: Date.now(), claimId: msg.correlationId, type: msg.type });
    if (this.entries.length > 500) this.entries.shift();
  };
}
