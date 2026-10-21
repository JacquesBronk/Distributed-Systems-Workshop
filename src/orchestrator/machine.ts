import { CLAIM_EVENTS, QUEUES, type Message, type Scenario } from '../messages.ts';

// The claim lifecycle as a pure function: (state, event) -> new state + messages to send.
// No broker, no database, no clock. This is what makes the orchestrator unit-testable (see test/machine.test.ts).
//
//   Submitted -> Assessing -> Approved -> RepairScheduled -> Paying -> Closed
//                   |  ^          ^
//                   v  |          |
//                FraudReview -----+          Rejected (end)     Cancelled (end)
//
//   Cancelling is the saga's compensation state: a cancellation arrived after a repairer was booked
//   (or while the booking was in flight) so we undo the business effect before closing.

export type State =
  | 'Submitted'
  | 'Assessing'
  | 'FraudReview'
  | 'Approved'
  | 'RepairScheduled'
  | 'Paying'
  | 'Cancelling'
  | 'Closed'
  | 'Rejected'
  | 'Cancelled';

export const TERMINAL: State[] = ['Closed', 'Rejected', 'Cancelled'];

export interface ClaimData {
  customer: string;
  description: string;
  amount: number;
  scenario: Scenario;
  // Assessing is a join: the assessment and the fraud check run in parallel, in either order.
  assessmentPassed: boolean;
  fraudCleared: boolean;
  cancelRequested: boolean;
  compensationSent: boolean;
  stateEpoch: number; // bumps on every state entry; timeouts carry it so stale ones are ignored
  attempts: number; // resends triggered by timeouts in the current state
  escalated: boolean; // timeouts exhausted: this claim needs a human
  reason?: string;
}

export interface ClaimRecord {
  id: string;
  state: State;
  version: number; // optimistic concurrency
  data: ClaimData;
}

/** A message the transition wants sent. It leaves via the outbox, only after the state is saved. */
export interface Out {
  route: string; // 'queue:<name>' | 'topic:<name>'
  kind: 'command' | 'event';
  type: string;
  body: Record<string, unknown>;
  delayMs?: number;
}

export type Outcome =
  | { kind: 'transition'; to: State; data: Partial<ClaimData>; out: Out[]; note?: string }
  | { kind: 'ignore'; reason: string } // duplicate, stale or irrelevant: ack and do nothing
  | { kind: 'defer'; reason: string; delayMs: number }; // too early: park it, try again shortly

export interface MachineConfig {
  timeouts: Partial<Record<State, number>>;
  maxResends: number;
}

export function freshClaim(msg: Message): ClaimRecord {
  const b = msg.body;
  return {
    id: msg.correlationId,
    state: 'Submitted',
    version: 0,
    data: {
      customer: b.customer,
      description: b.description,
      amount: b.amount,
      scenario: b.scenario,
      assessmentPassed: false,
      fraudCleared: false,
      cancelRequested: false,
      compensationSent: false,
      stateEpoch: 0,
      attempts: 0,
      escalated: false,
    },
  };
}

const cmd = (queue: string, type: string, body: Record<string, unknown>): Out => ({ route: `queue:${queue}`, kind: 'command', type, body });
const evt = (type: string, body: Record<string, unknown>): Out => ({ route: `topic:${CLAIM_EVENTS}`, kind: 'event', type, body });
const ignore = (reason: string): Outcome => ({ kind: 'ignore', reason });

const assessCmd = (r: ClaimRecord) =>
  cmd(QUEUES.assessment, 'AssessDamage', { claimId: r.id, amount: r.data.amount, description: r.data.description, outcome: r.data.scenario.assessment });
const fraudCmd = (r: ClaimRecord) => cmd(QUEUES.fraud, 'CheckFraud', { claimId: r.id, amount: r.data.amount, outcome: r.data.scenario.fraud });
const bookCmd = (r: ClaimRecord) => cmd(QUEUES.repairs, 'BookRepair', { claimId: r.id });
const cancelBookingCmd = (r: ClaimRecord) => cmd(QUEUES.repairs, 'CancelRepairBooking', { claimId: r.id });
const payCmd = (r: ClaimRecord) => cmd(QUEUES.payments, 'SettlePayment', { claimId: r.id, amount: r.data.amount });

function timeoutOut(r: ClaimRecord, state: State, epoch: number, cfg: MachineConfig): Out[] {
  const ms = cfg.timeouts[state];
  return ms ? [{ route: `queue:${QUEUES.timeouts}`, kind: 'command', type: 'TimeoutExpired', body: { claimId: r.id, state, epoch }, delayMs: ms }] : [];
}

/** Move to a new state: reset the per-state counters and arm the new state's deadline. */
function go(r: ClaimRecord, to: State, cfg: MachineConfig, out: Out[] = [], patch: Partial<ClaimData> = {}): Outcome {
  const epoch = r.data.stateEpoch + 1;
  return {
    kind: 'transition',
    to,
    data: { ...patch, stateEpoch: epoch, attempts: 0, escalated: false },
    out: [...out, ...timeoutOut(r, to, epoch, cfg)],
  };
}

/** Same state, new facts (e.g. one half of the Assessing join arrived). */
const stay = (r: ClaimRecord, patch: Partial<ClaimData>, note: string): Outcome => ({ kind: 'transition', to: r.state, data: patch, out: [], note });

const approve = (r: ClaimRecord, cfg: MachineConfig, patch: Partial<ClaimData>): Outcome =>
  go(r, 'Approved', cfg, [bookCmd(r), evt('ClaimApproved', { claimId: r.id })], patch);
const reject = (r: ClaimRecord, cfg: MachineConfig, reason: string): Outcome =>
  go(r, 'Rejected', cfg, [evt('ClaimRejected', { claimId: r.id, reason })], { reason });
const cancelled = (r: ClaimRecord, cfg: MachineConfig): Outcome => go(r, 'Cancelled', cfg, [evt('ClaimCancelled', { claimId: r.id })]);

export function decide(r: ClaimRecord | null, ev: Message, cfg: MachineConfig): Outcome {
  if (!r) return ignore('no such claim');

  if (TERMINAL.includes(r.state)) return ignore(`claim already ${r.state}`);
  if (ev.type === 'TimeoutExpired') return onTimeout(r, ev, cfg);

  const d = r.data;
  switch (r.state) {
    case 'Submitted': {
      if (ev.type !== 'ClaimSubmitted') return ignore('not expected in Submitted');
      if (!(d.amount > 0)) return reject(r, cfg, 'invalid claim amount');
      return go(r, 'Assessing', cfg, [assessCmd(r), fraudCmd(r)]);
    }

    case 'Assessing':
      switch (ev.type) {
        case 'AssessmentPassed':
          return d.fraudCleared ? approve(r, cfg, { assessmentPassed: true }) : stay(r, { assessmentPassed: true }, 'assessment passed, waiting for fraud result');
        case 'FraudCleared':
          // Card B: the fraud result can beat the assessment. The join accepts both orders.
          return d.assessmentPassed ? approve(r, cfg, { fraudCleared: true }) : stay(r, { fraudCleared: true }, 'fraud cleared first, waiting for assessment');
        case 'AssessmentFailed':
          return reject(r, cfg, 'assessment failed');
        case 'FraudSuspected':
          return go(r, 'FraudReview', cfg);
        case 'FraudConfirmed':
          return reject(r, cfg, 'fraud confirmed');
        case 'CancellationRequested':
          return cancelled(r, cfg);
      }
      return ignore(`${ev.type} not expected in Assessing`);

    case 'FraudReview':
      switch (ev.type) {
        case 'FraudCleared':
          return d.assessmentPassed ? approve(r, cfg, { fraudCleared: true }) : go(r, 'Assessing', cfg, [], { fraudCleared: true });
        case 'FraudConfirmed':
          return reject(r, cfg, 'fraud confirmed');
        case 'AssessmentPassed':
          return stay(r, { assessmentPassed: true }, 'assessment passed during fraud review');
        case 'AssessmentFailed':
          return reject(r, cfg, 'assessment failed');
        case 'CancellationRequested':
          return cancelled(r, cfg);
      }
      return ignore(`${ev.type} not expected in FraudReview`);

    case 'Approved': // BookRepair is in flight
      switch (ev.type) {
        case 'RepairBooked':
          return go(r, 'RepairScheduled', cfg);
        case 'CancellationRequested':
          // Card F: we can't recall the BookRepair command. Remember the cancel and compensate when the booking lands.
          return go(r, 'Cancelling', cfg, [], { cancelRequested: true, compensationSent: false });
        case 'RepairCompleted':
          return { kind: 'defer', reason: 'repair completed before we recorded the booking', delayMs: 100 };
      }
      return ignore(`${ev.type} not expected in Approved`);

    case 'RepairScheduled':
      switch (ev.type) {
        case 'RepairCompleted':
          return go(r, 'Paying', cfg, [payCmd(r)]);
        case 'CancellationRequested':
          // The saga compensation: undo the business effect of the earlier step.
          return go(r, 'Cancelling', cfg, [cancelBookingCmd(r)], { cancelRequested: true, compensationSent: true });
      }
      return ignore(`${ev.type} not expected in RepairScheduled`);

    case 'Paying':
      if (ev.type === 'PaymentSettled') return go(r, 'Closed', cfg, [evt('ClaimClosed', { claimId: r.id })]);
      if (ev.type === 'CancellationRequested') return ignore('too late to cancel: the repair is complete');
      return ignore(`${ev.type} not expected in Paying`);

    case 'Cancelling':
      if (ev.type === 'RepairBooked' && !d.compensationSent) {
        return { kind: 'transition', to: 'Cancelling', data: { compensationSent: true }, out: [cancelBookingCmd(r)], note: 'booking landed after the cancel: compensating' };
      }
      if (ev.type === 'RepairBookingCancelled') return cancelled(r, cfg);
      return ignore(`${ev.type} not expected in Cancelling`);
  }
  return ignore('unhandled');
}

/** Silence is the most dangerous failure: nothing errors. Only the state machine can notice it. */
function onTimeout(r: ClaimRecord, ev: Message, cfg: MachineConfig): Outcome {
  const { state, epoch } = ev.body;
  if (state !== r.state || epoch !== r.data.stateEpoch) return ignore('stale timeout: the claim has moved on');

  const resend: Out[] = [];
  const d = r.data;
  if (r.state === 'Assessing') {
    if (!d.assessmentPassed) resend.push(assessCmd(r));
    if (!d.fraudCleared) resend.push(fraudCmd(r));
  } else if (r.state === 'Approved') resend.push(bookCmd(r));
  else if (r.state === 'Paying') resend.push(payCmd(r));
  else if (r.state === 'Cancelling' && d.compensationSent) resend.push(cancelBookingCmd(r));

  if (resend.length && d.attempts < cfg.maxResends) {
    return {
      kind: 'transition',
      to: r.state,
      data: { attempts: d.attempts + 1 },
      out: [...resend, ...timeoutOut(r, r.state, d.stateEpoch, cfg)],
      note: `timeout: resending (${d.attempts + 1}/${cfg.maxResends})`,
    };
  }
  if (d.escalated) return ignore('already escalated');
  return {
    kind: 'transition',
    to: r.state,
    data: { escalated: true },
    out: [evt('ClaimEscalated', { claimId: r.id, state: r.state })],
    note: 'timeout: escalated to a human',
  };
}
