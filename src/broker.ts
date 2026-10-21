import { EventEmitter } from 'node:events';
import type { Message } from './messages.ts';

// A small in-memory broker with the semantics the workshop relies on, using Azure Service Bus
// vocabulary: topics with filtered subscriptions, queues, peek-lock, delivery counts, scheduled
// delivery, a dead-letter queue on every endpoint. It deliberately gives you what a real broker
// gives you and nothing more: at-least-once delivery, no global order, no duplicate protection.

export interface Delivery {
  msg: Message;
  deliveryCount: number; // 1 on first delivery
  complete(): void; // ack: remove for good
  abandon(delayMs?: number): void; // give it back, optionally not before delayMs (scheduled redelivery)
  deadLetter(reason: string): void; // park it for a human
}

export interface DeadLetter {
  msg: Message;
  reason: string;
  deliveryCount: number;
  at: number;
}

export interface LogEntry {
  t: number;
  op: string;
  endpoint: string;
  type: string;
  id: string;
  claimId: string;
  note?: string;
}

export interface EndpointStats {
  name: string;
  kind: 'queue' | 'subscription';
  topic?: string;
  ready: number;
  scheduled: number;
  inFlight: number;
  deadLettered: number;
  oldestAgeMs: number;
  delivered: number;
}

interface Entry {
  msg: Message;
  deliveryCount: number;
  enqueuedAt: number;
  availableAt: number;
  lockUntil: number; // 0 = not locked
  token: number;
  duplicateOnComplete: boolean;
}

interface Endpoint {
  name: string;
  kind: 'queue' | 'subscription';
  topic?: string;
  filter?: (msg: Message) => boolean;
  entries: Entry[];
  dlq: DeadLetter[];
  consumer?: { handler: (d: Delivery) => Promise<void>; concurrency: number; active: number };
  delivered: number;
}

export class Broker extends EventEmitter {
  private endpoints = new Map<string, Endpoint>();
  private topics = new Map<string, string[]>(); // topic -> subscription endpoint names
  private timer?: NodeJS.Timeout;
  private tokenSeq = 0;
  private duplicates: { endpoint: string; type: string }[] = [];
  readonly log: LogEntry[] = [];

  private readonly tickMs: number;
  private readonly lockMs: number;

  constructor(tickMs: number, lockMs: number) {
    super();
    this.tickMs = tickMs;
    this.lockMs = lockMs;
  }

  start() {
    this.timer = setInterval(() => this.tick(), this.tickMs);
  }

  stop() {
    clearInterval(this.timer);
  }

  declareQueue(name: string) {
    this.endpoints.set(name, { name, kind: 'queue', entries: [], dlq: [], delivered: 0 });
  }

  declareTopic(name: string) {
    this.topics.set(name, []);
  }

  subscribe(topic: string, name: string, filter?: (msg: Message) => boolean) {
    this.endpoints.set(name, { name, kind: 'subscription', topic, filter, entries: [], dlq: [], delivered: 0 });
    this.topics.get(topic)!.push(name);
  }

  /** Events: every matching subscription gets its own copy. Publishers never know the subscribers. */
  publish(topic: string, msg: Message, opts: { delayMs?: number } = {}) {
    const subs = this.topics.get(topic);
    if (!subs) throw new Error(`unknown topic ${topic}`);
    this.record('publish', topic, msg);
    for (const name of subs) {
      const ep = this.endpoints.get(name)!;
      if (!ep.filter || ep.filter(msg)) this.enqueue(ep, msg, opts.delayMs ?? 0);
    }
  }

  /** Commands: exactly one queue, one handler. */
  send(queue: string, msg: Message, opts: { delayMs?: number } = {}) {
    const ep = this.endpoints.get(queue);
    if (!ep || ep.kind !== 'queue') throw new Error(`unknown queue ${queue}`);
    this.record('send', queue, msg);
    this.enqueue(ep, msg, opts.delayMs ?? 0);
  }

  /** Route strings are 'queue:<name>' or 'topic:<name>', as stored in an outbox row. */
  route(route: string, msg: Message, opts: { delayMs?: number } = {}) {
    const [type, name] = route.split(':');
    if (type === 'topic') this.publish(name, msg, opts);
    else this.send(name, msg, opts);
  }

  consume(endpoint: string, handler: (d: Delivery) => Promise<void>, concurrency = 1) {
    const ep = this.endpoints.get(endpoint);
    if (!ep) throw new Error(`unknown endpoint ${endpoint}`);
    ep.consumer = { handler, concurrency, active: 0 };
  }

  /** Fault injection: the next matching message is processed, then redelivered, as if the pod died before the ack. */
  duplicateNext(endpoint: string, type: string) {
    this.duplicates.push({ endpoint, type });
  }

  deadLetters(endpoint: string): DeadLetter[] {
    return this.endpoints.get(endpoint)?.dlq ?? [];
  }

  /** The replay tool: put a dead-lettered message back, optionally with a repaired body. */
  replay(endpoint: string, id: string, body?: unknown) {
    const ep = this.endpoints.get(endpoint);
    const i = ep?.dlq.findIndex((d) => d.msg.id === id) ?? -1;
    if (!ep || i < 0) return false;
    const [dl] = ep.dlq.splice(i, 1);
    const msg = body === undefined ? dl.msg : { ...dl.msg, body };
    this.record('replay', endpoint, msg);
    this.enqueue(ep, msg, 0);
    return true;
  }

  discard(endpoint: string, id: string) {
    const ep = this.endpoints.get(endpoint);
    const i = ep?.dlq.findIndex((d) => d.msg.id === id) ?? -1;
    if (!ep || i < 0) return false;
    ep.dlq.splice(i, 1);
    this.emit('change');
    return true;
  }

  stats(): EndpointStats[] {
    const now = Date.now();
    return [...this.endpoints.values()].map((ep) => {
      const waiting = ep.entries.filter((e) => e.lockUntil === 0);
      return {
        name: ep.name,
        kind: ep.kind,
        topic: ep.topic,
        ready: waiting.filter((e) => e.availableAt <= now).length,
        scheduled: waiting.filter((e) => e.availableAt > now).length,
        inFlight: ep.entries.length - waiting.length,
        deadLettered: ep.dlq.length,
        oldestAgeMs: ep.entries.length ? now - Math.min(...ep.entries.map((e) => e.enqueuedAt)) : 0,
        delivered: ep.delivered,
      };
    });
  }

  /** True when nothing is queued, scheduled or in flight anywhere. Dead letters don't count. */
  idle() {
    return [...this.endpoints.values()].every((ep) => ep.entries.length === 0);
  }

  private enqueue(ep: Endpoint, msg: Message, delayMs: number, deliveryCount = 0) {
    const now = Date.now();
    ep.entries.push({
      msg: structuredClone(msg),
      deliveryCount,
      enqueuedAt: now,
      availableAt: now + delayMs,
      lockUntil: 0,
      token: 0,
      duplicateOnComplete: false,
    });
    this.emit('change');
  }

  private tick() {
    const now = Date.now();
    for (const ep of this.endpoints.values()) {
      for (const e of ep.entries) {
        if (e.lockUntil > 0 && e.lockUntil <= now) {
          // The handler outlived its lock: the broker assumes it died and hands the message to someone else.
          e.lockUntil = 0;
          e.availableAt = now;
          this.record('lock-expired', ep.name, e.msg, `delivery ${e.deliveryCount}`);
        }
      }
      const c = ep.consumer;
      if (!c) continue;
      while (c.active < c.concurrency) {
        const entry = ep.entries.find((e) => e.lockUntil === 0 && e.availableAt <= now);
        if (!entry) break;
        this.deliver(ep, entry);
      }
    }
  }

  private deliver(ep: Endpoint, entry: Entry) {
    const c = ep.consumer!;
    entry.deliveryCount++;
    entry.lockUntil = Date.now() + this.lockMs;
    const token = (entry.token = ++this.tokenSeq);
    ep.delivered++;
    const dupAt = this.duplicates.findIndex((d) => d.endpoint === ep.name && d.type === entry.msg.type);
    if (dupAt >= 0 && entry.deliveryCount === 1) {
      this.duplicates.splice(dupAt, 1);
      entry.duplicateOnComplete = true;
    }
    this.record('deliver', ep.name, entry.msg, `delivery ${entry.deliveryCount}`);

    // A settle only counts while the caller still holds the lock; late settles from a stale handler are ignored.
    const live = () => entry.token === token && entry.lockUntil > 0 && ep.entries.includes(entry);
    const remove = () => ep.entries.splice(ep.entries.indexOf(entry), 1);
    const delivery: Delivery = {
      msg: structuredClone(entry.msg),
      deliveryCount: entry.deliveryCount,
      complete: () => {
        if (!live()) return;
        remove();
        this.record('complete', ep.name, entry.msg);
        if (entry.duplicateOnComplete) {
          this.record('duplicate', ep.name, entry.msg, 'redelivered after ack was lost');
          this.enqueue(ep, entry.msg, 0, entry.deliveryCount);
        }
        this.emit('change');
      },
      abandon: (delayMs = 0) => {
        if (!live()) return;
        entry.lockUntil = 0;
        entry.availableAt = Date.now() + delayMs;
        this.record('abandon', ep.name, entry.msg, delayMs ? `retry in ${delayMs}ms` : 'retry now');
        this.emit('change');
      },
      deadLetter: (reason) => {
        if (!live()) return;
        remove();
        ep.dlq.push({ msg: entry.msg, reason, deliveryCount: entry.deliveryCount, at: Date.now() });
        this.record('dead-letter', ep.name, entry.msg, reason);
        this.emit('change');
      },
    };

    c.active++;
    c.handler(delivery)
      .catch((err) => delivery.abandon(0) ?? this.record('handler-error', ep.name, entry.msg, String(err)))
      .finally(() => c.active--);
  }

  private record(op: string, endpoint: string, msg: Message, note?: string) {
    this.log.push({ t: Date.now(), op, endpoint, type: msg.type, id: msg.id, claimId: msg.correlationId, note });
    if (this.log.length > 400) this.log.splice(0, this.log.length - 400);
    this.emit('change');
  }
}
