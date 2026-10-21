import { DatabaseSync } from 'node:sqlite';
import type { Broker } from '../broker.ts';
import { newMessage, type Message } from '../messages.ts';
import { Defer, Outbox, tx } from '../runtime.ts';
import { decide, freshClaim, type ClaimData, type ClaimRecord, type MachineConfig, type Out, type State } from './machine.ts';

export interface OrchestratorFaults {
  outboxEnabled: boolean; // off = naive "save, then publish" (the dual-write bug)
  crashNextCommit: boolean; // one-shot: die after the commit, before anything is sent
}

export interface HistoryRow {
  at: number;
  from: string;
  to: string;
  event: string;
  note: string | null;
}

class ConcurrencyError extends Error {}

/**
 * Runs the loop from the "anatomy of one transition" slide:
 *   1 event arrives -> 2 load instance + version -> 3 guard -> 4 transition and record, one transaction -> 5 messages leave
 * The machine never calls a service. It only reacts to messages and emits messages.
 */
export class Orchestrator {
  readonly db = new DatabaseSync(':memory:');
  readonly outbox: Outbox;
  private readonly broker: Broker;
  private readonly cfg: MachineConfig;
  private readonly faults: OrchestratorFaults;

  constructor(broker: Broker, cfg: MachineConfig, faults: OrchestratorFaults, outboxFaults: { relayCrashOnce: boolean }) {
    this.broker = broker;
    this.cfg = cfg;
    this.faults = faults;
    this.db.exec(`
      CREATE TABLE claims (id TEXT PRIMARY KEY, state TEXT NOT NULL, version INTEGER NOT NULL, data TEXT NOT NULL, created_at INTEGER NOT NULL);
      CREATE TABLE history (seq INTEGER PRIMARY KEY AUTOINCREMENT, claim_id TEXT NOT NULL, at INTEGER NOT NULL,
        from_state TEXT NOT NULL, to_state TEXT NOT NULL, event TEXT NOT NULL, message_id TEXT NOT NULL, note TEXT);
    `);
    this.outbox = new Outbox(this.db, broker, outboxFaults);
  }

  handle(msg: Message): void {
    const claimId = msg.correlationId;
    let sendDirect: [Out, Message][] = [];
    let sentAnything = false;

    tx(this.db, () => {
      const loaded = this.load(claimId);
      const rec = loaded ?? (msg.type === 'ClaimSubmitted' ? freshClaim(msg) : null);
      const outcome = decide(rec, msg, this.cfg);

      if (outcome.kind === 'defer') throw new Defer(outcome.reason, outcome.delayMs);
      if (outcome.kind === 'ignore') {
        if (rec) this.record(claimId, rec.state, rec.state, msg, `ignored: ${outcome.reason}`);
        return;
      }

      const next: ClaimData = { ...rec!.data, ...outcome.data };
      if (loaded) {
        // Optimistic concurrency: if another consumer moved this claim since we read it, we lose and retry.
        const res = this.db
          .prepare('UPDATE claims SET state = ?, version = version + 1, data = ? WHERE id = ? AND version = ?')
          .run(outcome.to, JSON.stringify(next), claimId, loaded.version);
        if (res.changes !== 1) throw new ConcurrencyError(`claim ${claimId} changed under us`);
      } else {
        this.db.prepare('INSERT INTO claims (id, state, version, data, created_at) VALUES (?, ?, 1, ?, ?)').run(claimId, outcome.to, JSON.stringify(next), Date.now());
      }
      this.record(claimId, rec!.state, outcome.to, msg, outcome.note ?? null);

      for (const o of outcome.out) {
        const out = newMessage(o.kind, o.type, o.body, claimId, msg.id);
        if (this.faults.outboxEnabled) this.outbox.add(o.route, out, o.delayMs ?? 0);
        else sendDirect.push([o, out]);
        sentAnything = true;
      }
    });

    // --- the dangerous gap: state is saved, nothing has been sent yet ---
    if (this.faults.crashNextCommit && sentAnything) {
      this.faults.crashNextCommit = false;
      // With the outbox the rows are already durable and the relay sends them. Without it they existed only in memory.
      throw new Error('simulated crash: process killed after commit, before publish');
    }
    for (const [o, out] of sendDirect) this.broker.route(o.route, out, { delayMs: o.delayMs });
  }

  private load(id: string): ClaimRecord | null {
    const row = this.db.prepare('SELECT id, state, version, data FROM claims WHERE id = ?').get(id) as
      | { id: string; state: State; version: number; data: string }
      | undefined;
    return row ? { id: row.id, state: row.state, version: row.version, data: JSON.parse(row.data) } : null;
  }

  private record(claimId: string, from: string, to: string, msg: Message, note: string | null) {
    this.db
      .prepare('INSERT INTO history (claim_id, at, from_state, to_state, event, message_id, note) VALUES (?, ?, ?, ?, ?, ?, ?)')
      .run(claimId, Date.now(), from, to, msg.type, msg.id, note);
  }

  /** The cheapest observability you'll ever build: every transition, queryable. */
  claims(limit = 50): (ClaimRecord & { createdAt: number })[] {
    const rows = this.db.prepare('SELECT id, state, version, data, created_at FROM claims ORDER BY created_at DESC, id DESC LIMIT ?').all(limit) as any[];
    return rows.map((r) => ({ id: r.id, state: r.state, version: r.version, data: JSON.parse(r.data), createdAt: r.created_at }));
  }

  claim(id: string): ClaimRecord | null {
    return this.load(id);
  }

  history(claimId: string): HistoryRow[] {
    return this.db
      .prepare('SELECT at, from_state AS "from", to_state AS "to", event, note FROM history WHERE claim_id = ? ORDER BY seq')
      .all(claimId) as unknown as HistoryRow[];
  }
}
