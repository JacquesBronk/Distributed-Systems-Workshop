import assert from 'node:assert/strict';
import { afterEach, describe, it } from 'node:test';
import type { System } from '../src/system.ts';
import { boot, ignored, path, sleep, stateOf, until, untilState } from './helpers.ts';

// One test per box on the workshop slides. Round 1 builds the happy path; round 2 is cards A to F.

let sys: System;
afterEach(() => sys?.stop());

describe('the happy path (the lifecycle slide)', () => {
  it('runs a claim from Submitted to Closed, orchestrating commands and events', async () => {
    sys = boot();
    const id = sys.submit({ amount: 3200 });
    await untilState(sys, id, 'Closed');

    assert.deepEqual(path(sys, id), ['Assessing', 'Assessing', 'Approved', 'RepairScheduled', 'Paying', 'Closed']);
    assert.equal(sys.payments.ledger().filter((p) => p.claimId === id).length, 1);
    // Notifications is choreographed: it reacted to events on its own subscription.
    await until(() => sys.notifications.sent.filter((n) => n.claimId === id).length === 2, 'approved + closed notifications');
    // Nothing is left stuck in the outbox or queues.
    await until(() => sys.broker.idle() && sys.snapshot().outboxPending === 0, 'queues to drain');
  });

  it('every message carries the ClaimId as its correlation id and names its cause', async () => {
    sys = boot();
    const id = sys.submit();
    await untilState(sys, id, 'Closed');
    const logged = sys.broker.log.filter((l) => l.claimId === id);
    assert.ok(logged.length > 10);
    assert.ok(logged.every((l) => l.claimId === id));
  });

  it('rejects a claim whose assessment fails (Assessing -> Rejected, no repair booked)', async () => {
    sys = boot();
    const id = sys.submit({ scenario: { assessment: 'fail', fraud: 'none' } });
    await untilState(sys, id, 'Rejected');
    await sleep(60);
    assert.equal(sys.repairs.bookings().length, 0);
    assert.ok(sys.notifications.sent.some((n) => n.claimId === id && n.text.includes('could not accept')));
  });

  it('routes a suspected fraud through FraudReview and back to Approved when cleared', async () => {
    sys = boot();
    const id = sys.submit({ scenario: { assessment: 'pass', fraud: 'review-clear' } });
    await untilState(sys, id, 'Closed');
    assert.ok(path(sys, id).includes('FraudReview'));
  });

  it('rejects a claim when the fraud review confirms fraud', async () => {
    sys = boot();
    const id = sys.submit({ scenario: { assessment: 'pass', fraud: 'review-confirm' } });
    await untilState(sys, id, 'Rejected');
    assert.equal(sys.repairs.bookings().length, 0);
  });
});

describe('Card A: PaymentSettled arrives twice', () => {
  it('the orchestrator guard acknowledges the duplicate and does nothing', async () => {
    sys = boot();
    sys.duplicateNext('orchestrator', 'PaymentSettled');
    const id = sys.submit();
    await untilState(sys, id, 'Closed');
    await until(() => ignored(sys, id, 'PaymentSettled').length === 1, 'the duplicate to be seen and ignored');
    assert.equal(stateOf(sys, id), 'Closed');
    await until(() => sys.broker.idle(), 'queues to drain');
    assert.equal(sys.notifications.sent.filter((n) => n.claimId === id && n.text.includes('settled')).length, 1);
  });

  it('the payments service is idempotent on its side: a redelivered SettlePayment pays once', async () => {
    sys = boot();
    sys.duplicateNext('payments', 'SettlePayment');
    const id = sys.submit();
    await untilState(sys, id, 'Closed');
    await until(() => sys.broker.idle(), 'redelivery to be processed');
    assert.equal(sys.payments.ledger().filter((p) => p.claimId === id).length, 1);
  });

  it('...and WITHOUT idempotency the customer is paid twice (why every handler must be safe to run twice)', async () => {
    sys = boot();
    sys.faults.paymentsIdempotent = false;
    sys.duplicateNext('payments', 'SettlePayment');
    const id = sys.submit();
    await untilState(sys, id, 'Closed');
    await until(() => sys.payments.ledger().filter((p) => p.claimId === id).length === 2, 'the double payment');
  });

  it('a timeout resend is a new MessageId, so the inbox alone is not enough: the natural key catches it', async () => {
    sys = boot();
    const id = sys.submit();
    await untilState(sys, id, 'Closed');
    // Replay a *different* message for the same claim, as a timeout resend would produce.
    sys.broker.send('payments', { id: 'resend-1', kind: 'command', type: 'SettlePayment', correlationId: id, body: { claimId: id, amount: 3200 }, at: Date.now() });
    await until(() => sys.broker.idle(), 'resend to be processed');
    assert.equal(sys.payments.ledger().filter((p) => p.claimId === id).length, 1);
  });
});

describe('Card B: FraudCleared arrives before the assessment has finished', () => {
  it('the Assessing join accepts both orders', async () => {
    sys = boot();
    sys.faults.assessmentDelayMs = 80; // fraud answers in a few ms; assessment is late
    const id = sys.submit();
    await until(() => sys.orchestrator.claim(id)?.data.fraudCleared === true, 'fraud result to land first');
    assert.equal(stateOf(sys, id), 'Assessing'); // waiting, not approved, not corrupted
    assert.equal(sys.orchestrator.claim(id)!.data.assessmentPassed, false);
    await untilState(sys, id, 'Closed');
  });
});

describe('Card C: repair booking is down', () => {
  it('a short outage is absorbed by retries and delayed redelivery, with nothing dead-lettered', async () => {
    sys = boot();
    sys.faults.repairDown = true;
    const id = sys.submit();
    await untilState(sys, id, 'Approved');
    await sleep(25);
    sys.faults.repairDown = false;
    await untilState(sys, id, 'Closed');
    assert.equal(sys.snapshot().deadLetters.length, 0);
    assert.ok(sys.broker.log.some((l) => l.op === 'abandon' && l.type === 'BookRepair'), 'BookRepair was retried');
  });

  it('a long outage dead-letters the command; the state timeout resends it; replaying the DLQ is harmless', async () => {
    sys = boot();
    sys.faults.repairDown = true;
    const id = sys.submit();
    await until(() => sys.snapshot().deadLetters.some((d) => d.type === 'BookRepair'), 'BookRepair to be dead-lettered');
    assert.equal(stateOf(sys, id), 'Approved'); // the claim is still waiting, which is what lets the timeout catch it
    sys.faults.repairDown = false;
    await untilState(sys, id, 'Closed', 6000); // the Approved deadline fired and resent the command

    // The ops team replays the dead letter after the fix. Idempotent booking + guard: no harm done.
    const dl = sys.snapshot().deadLetters.find((d) => d.type === 'BookRepair')!;
    assert.ok(sys.broker.replay(dl.endpoint, dl.id));
    await until(() => sys.broker.idle(), 'replay to be processed');
    assert.equal(sys.repairs.bookings().filter((b) => b.claimId === id).length, 1);
    assert.equal(stateOf(sys, id), 'Closed');
  });

  it('if the dependency never comes back, the claim escalates to a human instead of hanging silently', async () => {
    sys = boot();
    sys.faults.repairDown = true;
    const id = sys.submit();
    await until(() => sys.orchestrator.claim(id)?.data.escalated === true, 'escalation', 8000);
    assert.equal(stateOf(sys, id), 'Approved');
    await until(() => sys.notifications.sent.some((n) => n.claimId === id && n.text.includes('needs a human')), 'the escalation alert');
  });
});

describe('Card C, ops view: fix the dependency, replay the DLQ', () => {
  it('an escalated claim recovers once the provider is back and a dead letter is replayed', async () => {
    sys = boot();
    sys.faults.repairDown = true;
    const id = sys.submit();
    await until(() => sys.orchestrator.claim(id)?.data.escalated === true, 'escalation', 8000);
    await until(() => sys.snapshot().deadLetters.length >= 1, 'a dead letter');

    sys.faults.repairDown = false; // the fix
    const dl = sys.snapshot().deadLetters[0];
    sys.broker.replay(dl.endpoint, dl.id); // the replay tool
    await untilState(sys, id, 'Closed', 6000);
    assert.equal(sys.orchestrator.claim(id)!.data.escalated, false);
    assert.equal(sys.payments.ledger().filter((p) => p.claimId === id).length, 1);
  });
});

describe('Card D: a malformed message lands in the payments queue', () => {
  it('bounded retries, then the DLQ; real claims keep flowing; the poison message can be discarded', async () => {
    sys = boot();
    sys.injectPoison();
    const id = sys.submit(); // a healthy claim behind (or alongside) the poison message
    await untilState(sys, id, 'Closed');
    await until(() => sys.snapshot().deadLetters.length === 1, 'poison message to be dead-lettered');

    const [dl] = sys.snapshot().deadLetters;
    assert.equal(dl.endpoint, 'payments');
    assert.match(dl.reason, /amount must be a positive number/);
    assert.equal(sys.payments.ledger().length, 1); // only the real claim was paid
    assert.ok(sys.broker.discard(dl.endpoint, dl.id));
    assert.equal(sys.snapshot().deadLetters.length, 0);
  });

  it('a dead-lettered message can be repaired and replayed', async () => {
    sys = boot();
    sys.injectPoison();
    await until(() => sys.snapshot().deadLetters.length === 1, 'poison message to be dead-lettered');
    const [dl] = sys.snapshot().deadLetters;
    sys.broker.replay(dl.endpoint, dl.id, { claimId: 'CLM-FIXED', amount: 50 });
    await until(() => sys.payments.ledger().some((p) => p.claimId === 'CLM-FIXED'), 'repaired message to be processed');
  });
});

describe('Card E: the orchestrator crashes after saving state, before publishing', () => {
  it('WITH the outbox the message was saved in the same transaction; the relay sends it after the crash', async () => {
    sys = boot();
    sys.faults.crashNextCommit = true;
    const id = sys.submit();
    await untilState(sys, id, 'Closed');
    assert.equal(sys.faults.crashNextCommit, false); // the crash really happened
    assert.ok(sys.broker.log.some((l) => l.op === 'abandon' && l.type === 'ClaimSubmitted'), 'the crashed delivery was retried');
  });

  it('WITHOUT the outbox (save, then publish) the claim is stuck forever: the dual-write problem', async () => {
    sys = boot();
    sys.faults.outboxEnabled = false;
    sys.faults.crashNextCommit = true;
    const id = sys.submit();
    await until(() => sys.faults.crashNextCommit === false, 'the crash');
    await sleep(900); // longer than every deadline: even the timeout message was lost with the commands
    assert.equal(stateOf(sys, id), 'Assessing');
    assert.equal(sys.broker.log.filter((l) => l.claimId === id && l.type === 'AssessDamage').length, 0);
    assert.equal(sys.orchestrator.claim(id)!.data.escalated, false); // nobody even knows
    // Redelivery of ClaimSubmitted found the claim already in Assessing, so the guard ignored it.
    assert.equal(ignored(sys, id, 'ClaimSubmitted').length, 1);
  });

  it('a relay that dies after publishing, before marking sent, publishes twice; consumers shrug', async () => {
    sys = boot();
    sys.faults.relayCrashOnce = true;
    const id = sys.submit();
    await untilState(sys, id, 'Closed');
    assert.equal(sys.faults.relayCrashOnce, false);
    assert.equal(sys.payments.ledger().filter((p) => p.claimId === id).length, 1);
  });
});

describe('Card F: the customer cancels while a repairer is being booked', () => {
  it('cancelling early (Assessing) simply ends the claim', async () => {
    sys = boot();
    sys.faults.assessmentDelayMs = 100;
    const id = sys.submit();
    await untilState(sys, id, 'Assessing');
    sys.cancel(id);
    await untilState(sys, id, 'Cancelled');
    await sleep(150); // the late AssessmentPassed arrives in a terminal state and is ignored
    assert.equal(stateOf(sys, id), 'Cancelled');
    assert.equal(sys.repairs.bookings().length, 0);
  });

  it('a cancel that overtakes the in-flight BookRepair: record it, wait for the booking, then compensate (saga)', async () => {
    sys = boot();
    sys.faults.repairDelayMs = 80;
    const id = sys.submit();
    await untilState(sys, id, 'Approved');
    sys.cancel(id);
    await untilState(sys, id, 'Cancelling');
    await untilState(sys, id, 'Cancelled');
    assert.equal(sys.repairs.bookings().find((b) => b.claimId === id)?.status, 'cancelled');
    assert.equal(sys.payments.ledger().length, 0); // never paid
    assert.deepEqual(path(sys, id).slice(-3), ['Cancelling', 'Cancelling', 'Cancelled']);
  });

  it('cancelling after the repairer is booked sends the compensating command straight away', async () => {
    sys = boot();
    const id = sys.submit();
    await untilState(sys, id, 'RepairScheduled');
    sys.cancel(id);
    await untilState(sys, id, 'Cancelled');
    assert.equal(sys.repairs.bookings().find((b) => b.claimId === id)?.status, 'cancelled');
  });

  it('a cancel that arrives after the repair is complete is refused: too late', async () => {
    sys = boot();
    const id = sys.submit();
    await untilState(sys, id, 'Paying');
    sys.cancel(id);
    await untilState(sys, id, 'Closed');
    assert.equal(sys.payments.ledger().filter((p) => p.claimId === id).length, 1);
  });
});

describe('observability', () => {
  it('state history answers "where is my claim?" as a query', async () => {
    sys = boot();
    const id = sys.submit();
    await untilState(sys, id, 'Closed');
    const events = sys.orchestrator.history(id).map((h) => h.event);
    assert.equal(events[0], 'ClaimSubmitted');
    assert.deepEqual(events.slice(1, 3).sort(), ['AssessmentPassed', 'FraudCleared']); // the join accepts either order
    assert.ok(events.includes('PaymentSettled'));
  });
});
