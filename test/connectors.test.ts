import assert from 'node:assert/strict';
import { afterEach, describe, it } from 'node:test';
import { sign, verifySignature } from '../src/connectors/external.ts';
import type { System } from '../src/system.ts';
import { boot, sleep, stateOf, until, untilState } from './helpers.ts';

// The edge of the system: a provider's webhook comes in, an internal event goes out to partners, under rules.

let sys: System;
afterEach(() => sys?.stop());

const logged = (s: System, connector: string, outcome: string) => s.snapshot().connectors.log.filter((l) => l.connector === connector && l.outcome === outcome);
const published = (s: System, type: string, claimId: string) => s.broker.log.filter((l) => l.op === 'publish' && l.type === type && l.claimId === claimId);

describe('inbound connector: the garage network calls our webhook', () => {
  it('round trip: we dispatch the job in its format, it calls back in its format, we translate to RepairCompleted', async () => {
    sys = boot();
    const id = sys.submit({ customer: 'Dana Whitfield', description: 'Cracked radiator' });
    await untilState(sys, id, 'Closed');

    // What the provider received is ITS shape, not ours.
    const job = sys.garage.received.find((r) => r.path === '/jobs' && r.body.client_reference === id)!;
    assert.deepEqual(job.body, { client_reference: id, job_type: 'accident-repair', description: 'Cracked radiator', customer_name: 'Dana Whitfield' });
    assert.equal(job.key, `job:${id}`); // idempotency key is the business operation, not a message id

    // What entered our bus is OUR vocabulary, with an identity derived from the provider's event id.
    const [done] = published(sys, 'RepairCompleted', id);
    assert.ok(done.id.startsWith('garage-evt_'));
    assert.equal(logged(sys, 'garage-webhook', 'accepted').length, 1);
    // The orchestrator never learned that a garage exists: no provider field names leaked into the claim.
    assert.ok(!JSON.stringify(sys.orchestrator.claim(id)).includes('client_reference'));
  });

  it('rejects a bad signature at the edge: nothing reaches the bus', async () => {
    sys = boot();
    const raw = JSON.stringify({ event_id: 'evt_x', type: 'job.completed', client_reference: 'CLM-0001' });
    const res = sys.receiveGarageWebhook(raw, { 'x-timestamp': String(Date.now()), 'x-signature': 'forged' });
    assert.equal(res.status, 401);
    assert.equal(res.body.result, 'rejected');
    assert.equal(sys.broker.log.filter((l) => l.type === 'RepairCompleted').length, 0);
  });

  it('a rejected webhook is retried by the provider and the claim still completes', async () => {
    sys = boot();
    sys.faults.garageCorruptSignatureOnce = true;
    const id = sys.submit();
    await untilState(sys, id, 'Closed');
    assert.equal(logged(sys, 'garage-webhook', 'rejected').length, 1);
    assert.equal(logged(sys, 'garage-webhook', 'accepted').length, 1);
  });

  it('the signature covers a timestamp, so a captured request cannot be replayed later', () => {
    const raw = '{"a":1}';
    const old = String(Date.now() - 10 * 60_000);
    assert.equal(verifySignature('s3cret', old, sign('s3cret', old, raw), raw), false);
    const now = String(Date.now());
    assert.equal(verifySignature('s3cret', now, sign('s3cret', now, raw), raw), true);
    assert.equal(verifySignature('other', now, sign('s3cret', now, raw), raw), false);
    assert.equal(verifySignature('s3cret', now, sign('s3cret', now, raw), raw + ' '), false); // tampered body
  });

  it('de-duplicates on the provider event id: a webhook sent twice produces one internal event', async () => {
    sys = boot();
    sys.faults.garageDuplicateWebhooks = true;
    const id = sys.submit();
    await untilState(sys, id, 'Closed');
    await until(() => logged(sys, 'garage-webhook', 'duplicate').length === 1, 'the duplicate to be recognised');
    assert.equal(published(sys, 'RepairCompleted', id).length, 1);
    assert.equal(sys.payments.ledger().filter((p) => p.claimId === id).length, 1);
  });

  it('validates the provider schema at the edge: a malformed body is refused, not turned into a poison message', () => {
    sys = boot();
    const res = sys.sendStrayWebhook('malformed') as { status: number };
    assert.equal(res.status, 400);
    assert.equal(sys.snapshot().deadLetters.length, 0);
    assert.equal(sys.broker.log.filter((l) => l.type === 'RepairCompleted').length, 0);
  });

  it('quarantines an event for a job we never dispatched instead of guessing or dropping it silently', async () => {
    sys = boot();
    sys.sendStrayWebhook('unknown-job');
    const q = sys.snapshot().connectors.quarantine;
    assert.equal(q.length, 1);
    assert.equal(q[0].clientReference, 'CLM-9999');
    await sleep(30);
    assert.equal(sys.broker.log.filter((l) => l.type === 'RepairCompleted').length, 0);
  });

  it('when the garage API is down the dispatch is retried and the repair still happens', async () => {
    sys = boot();
    sys.faults.garageDown = true;
    const id = sys.submit();
    await until(() => logged(sys, 'garage-dispatch', 'failed').length >= 1, 'a failed dispatch');
    sys.faults.garageDown = false;
    await untilState(sys, id, 'Closed', 6000);
    assert.equal(sys.garage.received.filter((r) => r.path === '/jobs' && !r.duplicate).length, 1);
  });

  it('cancelling a claim cancels the job at the garage, and a late completion changes nothing', async () => {
    sys = boot();
    const id = sys.submit();
    await untilState(sys, id, 'RepairScheduled');
    sys.cancel(id);
    await untilState(sys, id, 'Cancelled');
    await until(() => sys.garage.received.some((r) => r.path === '/jobs/cancel' && r.body.client_reference === id), 'the cancel to reach the garage');
    await sleep(120);
    assert.equal(stateOf(sys, id), 'Cancelled');
    assert.equal(sys.payments.ledger().length, 0);
  });
});

describe('outbound connector: rules decide what leaves, and in what shape', () => {
  it('a large claim is reported to the reinsurer in the reinsurer’s format', async () => {
    sys = boot();
    const id = sys.submit({ amount: 6500 });
    await untilState(sys, id, 'Closed');
    await until(() => sys.reinsurer.received.length === 1, 'the reinsurer report');
    const sent = sys.reinsurer.received[0];
    assert.equal(sent.path, '/bordereau');
    assert.deepEqual({ ...sent.body, closed_at: 'x' }, { claim_ref: id, paid_pence: 650000, currency: 'GBP', closed_at: 'x' });
    assert.ok(!('customer' in sent.body)); // only what the partner is entitled to
  });

  it('a small claim is deliberately not reported, and the decision is logged with the rule that made it', async () => {
    sys = boot();
    const id = sys.submit({ amount: 2400 });
    await untilState(sys, id, 'Closed');
    await until(() => logged(sys, 'partner-reporting', 'skipped').length === 1, 'the skip decision');
    assert.match(logged(sys, 'partner-reporting', 'skipped')[0].detail, /below the £5000 reporting threshold \[small-claims\]/);
    assert.equal(sys.reinsurer.received.length, 0);
  });

  it('routes by rule: confirmed fraud goes to the fraud bureau, not the reinsurer', async () => {
    sys = boot();
    const id = sys.submit({ amount: 8000, scenario: { assessment: 'pass', fraud: 'review-confirm' } });
    await untilState(sys, id, 'Rejected');
    await until(() => sys.fraudBureau.received.length === 1, 'the fraud bureau case');
    assert.deepEqual(sys.fraudBureau.received[0].body, { case_ref: id, reason: 'fraud confirmed', claimed_amount_gbp: 8000 });
    assert.equal(sys.reinsurer.received.length, 0);
  });

  it('cancelled claims are not reported', async () => {
    sys = boot();
    sys.faults.assessmentDelayMs = 100;
    const id = sys.submit({ amount: 9000 });
    await untilState(sys, id, 'Assessing');
    sys.cancel(id);
    await untilState(sys, id, 'Cancelled');
    await until(() => logged(sys, 'partner-reporting', 'skipped').length === 1, 'the skip decision');
    assert.equal(sys.reinsurer.received.length, 0);
  });

  it('a duplicate delivery reaches the partner as a recognised duplicate, not a second report', async () => {
    sys = boot();
    sys.duplicateNext('partner-reporting', 'ClaimClosed');
    const id = sys.submit({ amount: 7000 });
    await untilState(sys, id, 'Closed');
    await until(() => sys.reinsurer.received.length === 2, 'both deliveries to reach the partner');
    assert.deepEqual(sys.reinsurer.received.map((r) => r.duplicate), [false, true]);
    assert.equal(new Set(sys.reinsurer.received.map((r) => r.key)).size, 1); // same business key, so the partner dedupes
    assert.equal(id, sys.reinsurer.received[0].body.claim_ref);
  });

  it('a partner outage opens the circuit breaker: we stop calling, keep the message, and send it once the partner recovers', async () => {
    sys = boot();
    sys.faults.reinsurerDown = true;
    const id = sys.submit({ amount: 6000 });
    await untilState(sys, id, 'Closed');
    await until(() => logged(sys, 'partner-reporting', 'circuit-open').length >= 1, 'the circuit to open');
    assert.ok(logged(sys, 'partner-reporting', 'failed').length >= 3);
    assert.ok(sys.snapshot().connectors.circuits.some((c) => c.target === 'reinsurer' && c.state !== 'closed'));
    // The claim itself was never held up: the partner's problem stays in the partner's queue.
    assert.equal(stateOf(sys, id), 'Closed');

    sys.faults.reinsurerDown = false;
    await until(() => sys.reinsurer.received.length === 1, 'the report after recovery', 6000);
    assert.equal(sys.snapshot().deadLetters.length, 0);
    await until(() => sys.snapshot().connectors.circuits.every((c) => c.state === 'closed'), 'the circuit to close');
  });

  it('a long partner outage dead-letters the report; replaying it after the fix delivers it exactly once', async () => {
    sys = boot();
    sys.faults.reinsurerDown = true;
    const id = sys.submit({ amount: 6000 });
    await untilState(sys, id, 'Closed');
    await until(() => sys.snapshot().deadLetters.some((d) => d.endpoint === 'partner-reporting'), 'the dead letter', 6000);

    sys.faults.reinsurerDown = false;
    const dl = sys.snapshot().deadLetters.find((d) => d.endpoint === 'partner-reporting')!;
    assert.ok(sys.broker.replay(dl.endpoint, dl.id));
    await until(() => sys.reinsurer.received.some((r) => r.body.claim_ref === id), 'the replayed report');
    assert.equal(sys.reinsurer.received.filter((r) => !r.duplicate).length, 1);
  });
});
