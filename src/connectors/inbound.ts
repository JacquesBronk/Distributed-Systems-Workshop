import { DatabaseSync } from 'node:sqlite';
import type { Broker } from '../broker.ts';
import { CLAIM_EVENTS, newMessage } from '../messages.ts';
import { Outbox, tx } from '../runtime.ts';
import { verifySignature } from './external.ts';

export interface ConnectorLogEntry {
  at: number;
  connector: string;
  direction: 'in' | 'out';
  outcome: string; // accepted | duplicate | rejected | quarantined | sent | skipped | failed | circuit-open
  detail: string;
}

export class ConnectorLog {
  readonly entries: ConnectorLogEntry[] = [];
  add(connector: string, direction: 'in' | 'out', outcome: string, detail: string) {
    this.entries.push({ at: Date.now(), connector, direction, outcome, detail });
    if (this.entries.length > 200) this.entries.shift();
  }
}

export interface HttpResult {
  status: number;
  body: { result: string; detail?: string };
}

/**
 * Inbound connector: where an outside system's event enters ours. It is an anti-corruption layer.
 * Nothing the provider sends reaches the bus until it has been authenticated, validated,
 * de-duplicated on the provider's own id, and translated into OUR vocabulary:
 *
 *   webhook -> 1 verify signature -> 2 validate shape -> 3 de-duplicate on provider event id
 *           -> 4 translate (job.completed -> RepairCompleted) -> 5 save + outbox, one transaction -> 6 acknowledge fast
 *
 * The provider's format, ids and quirks stop here. The orchestrator never learns that a garage exists.
 */
export class GarageInbound {
  readonly db = new DatabaseSync(':memory:');
  readonly outbox: Outbox;
  private readonly secret: string;
  private readonly log: ConnectorLog;
  private static readonly NAME = 'garage-webhook';

  constructor(broker: Broker, secret: string, log: ConnectorLog, outboxFaults: { relayCrashOnce: boolean }) {
    this.secret = secret;
    this.log = log;
    this.db.exec(`
      CREATE TABLE inbox (event_id TEXT PRIMARY KEY, received_at INTEGER NOT NULL);
      CREATE TABLE dispatched (client_reference TEXT PRIMARY KEY, at INTEGER NOT NULL);
      CREATE TABLE quarantine (seq INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT NOT NULL, client_reference TEXT NOT NULL, reason TEXT NOT NULL, at INTEGER NOT NULL);
    `);
    this.outbox = new Outbox(this.db, broker, outboxFaults);
  }

  /** The outbound side tells us which jobs exist, so we can recognise (and quarantine) strangers. */
  recordDispatched(clientReference: string) {
    this.db.prepare('INSERT OR IGNORE INTO dispatched (client_reference, at) VALUES (?, ?)').run(clientReference, Date.now());
  }

  handle(rawBody: string, headers: Record<string, string | undefined>): HttpResult {
    // 1. Authenticate before parsing anything. An unauthenticated caller learns nothing and costs us nothing.
    if (!verifySignature(this.secret, headers['x-timestamp'], headers['x-signature'], rawBody)) {
      this.log.add(GarageInbound.NAME, 'in', 'rejected', '401 bad or missing signature');
      return { status: 401, body: { result: 'rejected', detail: 'invalid signature' } };
    }

    // 2. Validate the provider's schema at the edge, so a malformed payload never becomes a poison message on the bus.
    let event: any;
    try {
      event = JSON.parse(rawBody);
    } catch {
      this.log.add(GarageInbound.NAME, 'in', 'rejected', '400 body is not valid JSON');
      return { status: 400, body: { result: 'rejected', detail: 'invalid JSON' } };
    }
    if (typeof event?.event_id !== 'string' || typeof event?.type !== 'string' || typeof event?.client_reference !== 'string') {
      this.log.add(GarageInbound.NAME, 'in', 'rejected', '422 missing event_id, type or client_reference');
      return { status: 422, body: { result: 'rejected', detail: 'schema' } };
    }

    // Event types we have no use for are acknowledged and dropped, so the provider stops retrying them.
    if (event.type !== 'job.completed') {
      this.log.add(GarageInbound.NAME, 'in', 'skipped', `${event.type} for ${event.client_reference}: not mapped`);
      return { status: 200, body: { result: 'ignored' } };
    }

    const claimId: string = event.client_reference;
    const outcome = tx(this.db, () => {
      // 3. Providers redeliver. Their event id is the stable identity: first sighting wins.
      const fresh = this.db.prepare('INSERT OR IGNORE INTO inbox (event_id, received_at) VALUES (?, ?)').run(event.event_id, Date.now());
      if (fresh.changes === 0) return 'duplicate' as const;

      // Unknown job: don't guess and don't drop it silently. Park it where a person can look.
      if (!this.db.prepare('SELECT 1 FROM dispatched WHERE client_reference = ?').get(claimId)) {
        this.db.prepare('INSERT INTO quarantine (event_id, client_reference, reason, at) VALUES (?, ?, ?, ?)').run(event.event_id, claimId, 'no job was dispatched for this reference', Date.now());
        return 'quarantined' as const;
      }

      // 4. Translate: their vocabulary in, ours out. The internal message id derives from THEIR event id,
      //    so even a replay through the relay keeps a stable identity.
      const internal = { ...newMessage('event', 'RepairCompleted', { claimId, source: 'garage-network', externalEventId: event.event_id }, claimId), id: `garage-${event.event_id}` };
      // 5. Saved with the inbox row in this same transaction: either we have both or neither.
      this.outbox.add(`topic:${CLAIM_EVENTS}`, internal);
      return 'accepted' as const;
    });

    // 6. Acknowledge once it is safely stored. The slow work happens later, off the provider's clock.
    const detail = { duplicate: `${event.event_id}: already processed`, quarantined: `${event.event_id}: unknown job ${claimId}`, accepted: `${event.type} ${claimId} -> RepairCompleted` }[outcome];
    this.log.add(GarageInbound.NAME, 'in', outcome, detail);
    return { status: outcome === 'accepted' ? 202 : 200, body: { result: outcome } };
  }

  quarantined() {
    return this.db.prepare('SELECT event_id AS eventId, client_reference AS clientReference, reason, at FROM quarantine ORDER BY seq DESC').all() as { eventId: string; clientReference: string; reason: string; at: number }[];
  }
}
