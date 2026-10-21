import { createHmac, timingSafeEqual } from 'node:crypto';
import { rand, sleep } from '../messages.ts';

// Stand-ins for systems we don't own. They behave the way real providers do: they go down, they
// de-duplicate on the idempotency key we send, and they call us back with webhooks that are
// signed, retried and occasionally sent twice.

export const WEBHOOK_TOLERANCE_MS = 5 * 60_000;

export const sign = (secret: string, timestamp: string, rawBody: string) => createHmac('sha256', secret).update(`${timestamp}.${rawBody}`).digest('hex');

/** Constant-time check of the HMAC, and a timestamp window so a captured request can't be replayed later. */
export function verifySignature(secret: string, timestamp: string | undefined, signature: string | undefined, rawBody: string, now = Date.now()): boolean {
  if (!timestamp || !signature) return false;
  if (Math.abs(now - Number(timestamp)) > WEBHOOK_TOLERANCE_MS) return false;
  const expected = Buffer.from(sign(secret, timestamp, rawBody));
  const given = Buffer.from(signature);
  return expected.length === given.length && timingSafeEqual(expected, given);
}

export class ProviderError extends Error {
  readonly status: number;
  constructor(message: string, status: number) {
    super(message);
    this.status = status;
  }
}

export interface ProviderRequest {
  path: string;
  idempotencyKey: string; // a business key, so a re-sent request is recognised as the same operation
  body: Record<string, unknown>;
}

export interface ProviderRecord {
  at: number;
  path: string;
  key: string;
  duplicate: boolean;
  body: Record<string, unknown>;
}

export class ExternalProvider {
  readonly name: string;
  readonly received: ProviderRecord[] = [];
  private readonly keys = new Set<string>();
  private readonly isDown: () => boolean;
  private readonly latency: [number, number];

  constructor(name: string, isDown: () => boolean, latency: [number, number]) {
    this.name = name;
    this.isDown = isDown;
    this.latency = latency;
  }

  /** The provider's API. Throws like an HTTP client would when the provider is down. */
  async call(req: ProviderRequest): Promise<{ status: number; duplicate: boolean }> {
    await sleep(rand(this.latency[0] / 2, this.latency[1] / 2));
    if (this.isDown()) throw new ProviderError(`${this.name} returned 503 Service Unavailable`, 503);
    const duplicate = this.keys.has(req.idempotencyKey);
    this.keys.add(req.idempotencyKey);
    this.received.push({ at: Date.now(), path: req.path, key: req.idempotencyKey, duplicate, body: req.body });
    if (this.received.length > 100) this.received.shift();
    if (!duplicate) this.onAccepted(req);
    return { status: duplicate ? 200 : 201, duplicate };
  }

  protected onAccepted(_req: ProviderRequest): void {}

  stats() {
    return { name: this.name, down: this.isDown(), received: this.received.length, duplicates: this.received.filter((r) => r.duplicate).length };
  }
}

export interface GarageFaults {
  garageDuplicateWebhooks: boolean; // the provider sends every webhook twice
  garageCorruptSignatureOnce: boolean; // the next webhook is signed wrongly, then retried properly
}

export type WebhookSink = (rawBody: string, headers: Record<string, string>) => { status: number; body: unknown };

/**
 * The repairers' network. We send it jobs (outbound connector); it tells us when they are done by
 * calling our webhook (inbound connector), in ITS format, with ITS ids. It retries on non-2xx.
 */
export class GarageNetwork extends ExternalProvider {
  private readonly secret: string;
  private readonly faults: GarageFaults;
  private readonly sink: WebhookSink;
  private readonly repairMs: number;
  private readonly cancelled = new Set<string>();
  private eventSeq = 0;

  constructor(opts: { secret: string; isDown: () => boolean; faults: GarageFaults; sink: WebhookSink; latency: [number, number]; repairMs: number }) {
    super('garage-network', opts.isDown, opts.latency);
    this.secret = opts.secret;
    this.faults = opts.faults;
    this.sink = opts.sink;
    this.repairMs = opts.repairMs;
  }

  protected override onAccepted(req: ProviderRequest) {
    const ref = String(req.body.client_reference);
    if (req.path === '/jobs/cancel') return void this.cancelled.add(ref);
    setTimeout(() => {
      if (!this.cancelled.has(ref)) this.webhook({ type: 'job.completed', client_reference: ref });
    }, this.repairMs);
  }

  /** Public so the dashboard can fire webhooks for jobs we never sent. */
  webhook(event: { type: string; client_reference: string }) {
    const payload = { event_id: `evt_${++this.eventSeq}_${Math.random().toString(36).slice(2, 7)}`, occurred_at: new Date().toISOString(), ...event };
    const raw = JSON.stringify(payload);
    const post = (valid: boolean) => {
      const ts = String(Date.now());
      const good = sign(this.secret, ts, raw);
      return this.sink(raw, { 'x-timestamp': ts, 'x-signature': valid ? good : `bad${good.slice(3)}` });
    };
    if (this.faults.garageCorruptSignatureOnce) {
      this.faults.garageCorruptSignatureOnce = false;
      post(false);
      // Real providers retry a rejected webhook, with a correct signature this time.
      setTimeout(() => post(true), Math.max(20, this.repairMs / 3));
    } else post(true);
    if (this.faults.garageDuplicateWebhooks) post(true);
  }

  /** A syntactically broken body, signed correctly: it passes authentication and fails validation. */
  malformedWebhook() {
    const raw = '{"event_id": "evt_bad", "type": 42';
    const ts = String(Date.now());
    return this.sink(raw, { 'x-timestamp': ts, 'x-signature': sign(this.secret, ts, raw) });
  }
}
