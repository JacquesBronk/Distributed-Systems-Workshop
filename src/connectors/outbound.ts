import type { Timing } from '../config.ts';
import type { Message } from '../messages.ts';
import { Defer } from '../runtime.ts';
import type { ExternalProvider } from './external.ts';
import type { ConnectorLog } from './inbound.ts';

/**
 * A rule decides what happens to one internal event on its way out. First match wins.
 * `send` rules name a target provider and say how to translate the event into the provider's request;
 * `skip` rules say why a message deliberately goes nowhere.
 */
export type Rule =
  | {
      name: string;
      action: 'send';
      when: (m: Message) => boolean;
      target: string;
      path: string;
      idempotencyKey: (m: Message) => string; // a business key, NOT the message id
      transform: (m: Message) => Record<string, unknown>;
      after?: (m: Message) => void; // hook once the provider has accepted
    }
  | { name: string; action: 'skip'; when: (m: Message) => boolean; reason: string };

interface Breaker {
  failures: number;
  openUntil: number;
}

/**
 * Outbound connector: an internal event goes to an outside system.
 *
 *   event -> 1 rules (filter, route) -> 2 transform to the provider's shape -> 3 circuit breaker
 *         -> 4 call with an idempotency key -> 5 retry / dead-letter on failure -> 6 log every decision
 *
 * It is just another consumer: its own subscription, its own DLQ, so a slow or dead partner backs up
 * ITS queue and nothing else.
 */
export class OutboundConnector {
  readonly name: string;
  private readonly rules: Rule[];
  private readonly providers: Record<string, ExternalProvider>;
  private readonly log: ConnectorLog;
  private readonly threshold: number;
  private readonly cooldownMs: number;
  private readonly breakers = new Map<string, Breaker>();

  constructor(name: string, rules: Rule[], providers: Record<string, ExternalProvider>, log: ConnectorLog, timing: Timing) {
    this.name = name;
    this.rules = rules;
    this.providers = providers;
    this.log = log;
    this.threshold = timing.breakerThreshold;
    this.cooldownMs = timing.breakerCooldownMs;
  }

  handle = async (msg: Message): Promise<void> => {
    const rule = this.rules.find((r) => r.when(msg));
    if (!rule) {
      this.log.add(this.name, 'out', 'skipped', `${msg.type} ${msg.correlationId}: no rule matched`);
      return;
    }
    if (rule.action === 'skip') {
      this.log.add(this.name, 'out', 'skipped', `${msg.type} ${msg.correlationId}: ${rule.reason} [${rule.name}]`);
      return;
    }

    const breaker = this.breakerFor(rule.target);
    const now = Date.now();
    if (breaker.failures >= this.threshold && now < breaker.openUntil) {
      // Open circuit: don't even try. Protect the partner, and don't burn our retry budget on a known outage.
      this.log.add(this.name, 'out', 'circuit-open', `${rule.target}: holding ${msg.type} ${msg.correlationId}`);
      throw new Defer(`circuit open for ${rule.target}`, breaker.openUntil - now);
    }

    try {
      const res = await this.providers[rule.target].call({ path: rule.path, idempotencyKey: rule.idempotencyKey(msg), body: rule.transform(msg) });
      breaker.failures = 0;
      this.log.add(this.name, 'out', 'sent', `${msg.type} ${msg.correlationId} -> ${rule.target}${res.duplicate ? ' (provider recognised a duplicate)' : ''} [${rule.name}]`);
      rule.after?.(msg);
    } catch (err) {
      if (++breaker.failures >= this.threshold) breaker.openUntil = Date.now() + this.cooldownMs;
      this.log.add(this.name, 'out', 'failed', `${msg.type} ${msg.correlationId} -> ${rule.target}: ${err instanceof Error ? err.message : err}`);
      throw err; // the consumer's retry ladder takes it from here: retry, backoff, dead-letter
    }
  };

  circuits() {
    return [...this.breakers.entries()].map(([target, b]) => ({
      connector: this.name,
      target,
      failures: b.failures,
      state: b.failures < this.threshold ? 'closed' : Date.now() < b.openUntil ? 'open' : 'half-open',
    }));
  }

  private breakerFor(target: string): Breaker {
    let b = this.breakers.get(target);
    if (!b) this.breakers.set(target, (b = { failures: 0, openUntil: 0 }));
    return b;
  }
}
