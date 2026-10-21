import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { decide, freshClaim, type ClaimRecord, type MachineConfig, type State } from '../src/orchestrator/machine.ts';
import { newMessage } from '../src/messages.ts';

// "Given a state and an event, it produces a new state and a list of messages.
//  That is a pure function you can unit test without a broker."

const cfg: MachineConfig = { timeouts: { Assessing: 1000, Approved: 1000, Paying: 1000 }, maxResends: 2 };
const submitted = newMessage('event', 'ClaimSubmitted', { customer: 'c', description: 'd', amount: 100, scenario: { assessment: 'pass', fraud: 'none' } }, 'CLM-1');
const claim = (state: State, data: Partial<ClaimRecord['data']> = {}): ClaimRecord => {
  const c = freshClaim(submitted);
  return { ...c, state, version: 3, data: { ...c.data, ...data } };
};
const ev = (type: string, body: Record<string, unknown> = {}) => newMessage('event', type, { claimId: 'CLM-1', ...body }, 'CLM-1');

describe('claim state machine', () => {
  it('a new claim enters Assessing and sends AssessDamage and CheckFraud, plus an armed deadline', () => {
    const o = decide(freshClaim(submitted), submitted, cfg);
    assert.equal(o.kind, 'transition');
    if (o.kind !== 'transition') return;
    assert.equal(o.to, 'Assessing');
    assert.deepEqual(o.out.map((m) => m.type), ['AssessDamage', 'CheckFraud', 'TimeoutExpired']);
  });

  it('rejects an invalid amount without sending any command', () => {
    const bad = newMessage('event', 'ClaimSubmitted', { ...submitted.body, amount: 0 }, 'CLM-1');
    const o = decide(freshClaim(bad), bad, cfg);
    assert.ok(o.kind === 'transition' && o.to === 'Rejected');
    assert.deepEqual(o.kind === 'transition' && o.out.map((m) => m.type), ['ClaimRejected']);
  });

  it('commands and events are named by the rules: imperative vs past tense, and routed to a queue vs a topic', () => {
    const o = decide(claim('Assessing', { assessmentPassed: true }), ev('FraudCleared'), cfg);
    assert.ok(o.kind === 'transition');
    for (const m of o.out) {
      if (m.kind === 'command') assert.ok(m.route.startsWith('queue:'));
      else assert.ok(m.route.startsWith('topic:'));
    }
    assert.deepEqual(o.out.map((m) => m.type), ['BookRepair', 'ClaimApproved', 'TimeoutExpired']);
  });

  it('is symmetric: both orders of AssessmentPassed / FraudCleared reach Approved', () => {
    const a = decide(claim('Assessing', { fraudCleared: true }), ev('AssessmentPassed'), cfg);
    const b = decide(claim('Assessing', { assessmentPassed: true }), ev('FraudCleared'), cfg);
    assert.ok(a.kind === 'transition' && a.to === 'Approved');
    assert.ok(b.kind === 'transition' && b.to === 'Approved');
  });

  it('ignores duplicates and events from the future instead of corrupting the claim', () => {
    assert.equal(decide(claim('Closed'), ev('PaymentSettled'), cfg).kind, 'ignore');
    assert.equal(decide(claim('Assessing'), ev('PaymentSettled'), cfg).kind, 'ignore'); // PaymentSettled while still Assessing
    assert.equal(decide(claim('RepairScheduled'), ev('RepairBooked'), cfg).kind, 'ignore');
    assert.equal(decide(null, ev('PaymentSettled'), cfg).kind, 'ignore');
  });

  it('defers RepairCompleted if it overtakes RepairBooked', () => {
    assert.equal(decide(claim('Approved'), ev('RepairCompleted'), cfg).kind, 'defer');
  });

  it('a cancellation is handled differently in every state', () => {
    const to = (s: State) => {
      const o = decide(claim(s), ev('CancellationRequested'), cfg);
      return o.kind === 'transition' ? o.to : o.kind;
    };
    assert.equal(to('Assessing'), 'Cancelled');
    assert.equal(to('FraudReview'), 'Cancelled');
    assert.equal(to('Approved'), 'Cancelling');
    assert.equal(to('RepairScheduled'), 'Cancelling');
    assert.equal(to('Paying'), 'ignore');
    assert.equal(to('Closed'), 'ignore');
  });

  it('compensates by command: CancelRepairBooking once the booking is known to exist', () => {
    const fromScheduled = decide(claim('RepairScheduled'), ev('CancellationRequested'), cfg);
    assert.ok(fromScheduled.kind === 'transition' && fromScheduled.out.some((m) => m.type === 'CancelRepairBooking'));
    const lateBooking = decide(claim('Cancelling', { compensationSent: false }), ev('RepairBooked'), cfg);
    assert.ok(lateBooking.kind === 'transition' && lateBooking.out.some((m) => m.type === 'CancelRepairBooking'));
    const repeat = decide(claim('Cancelling', { compensationSent: true }), ev('RepairBooked'), cfg);
    assert.equal(repeat.kind, 'ignore');
  });

  it('entering a new state clears an earlier escalation: the claim recovered', () => {
    const o = decide(claim('Approved', { escalated: true, attempts: 2 }), ev('RepairBooked'), cfg);
    assert.ok(o.kind === 'transition' && o.data.escalated === false && o.data.attempts === 0);
  });

  describe('timeouts', () => {
    const timeout = (state: State, epoch: number) => newMessage('command', 'TimeoutExpired', { claimId: 'CLM-1', state, epoch }, 'CLM-1');

    it('a stale timeout (the claim has moved on) is ignored', () => {
      assert.equal(decide(claim('Paying', { stateEpoch: 5 }), timeout('Paying', 4), cfg).kind, 'ignore');
      assert.equal(decide(claim('Closed', { stateEpoch: 5 }), timeout('Paying', 5), cfg).kind, 'ignore');
    });

    it('resends the command up to maxResends, then escalates once', () => {
      let c = claim('Paying', { stateEpoch: 5 });
      for (let i = 1; i <= cfg.maxResends; i++) {
        const o = decide(c, timeout('Paying', 5), cfg);
        assert.ok(o.kind === 'transition' && o.out.some((m) => m.type === 'SettlePayment'));
        c = { ...c, data: { ...c.data, ...(o as any).data } };
        assert.equal(c.data.attempts, i);
      }
      const esc = decide(c, timeout('Paying', 5), cfg);
      assert.ok(esc.kind === 'transition' && esc.out.some((m) => m.type === 'ClaimEscalated'));
      c = { ...c, data: { ...c.data, ...(esc as any).data } };
      assert.equal(decide(c, timeout('Paying', 5), cfg).kind, 'ignore');
    });
  });
});
