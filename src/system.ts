import { Broker } from './broker.ts';
import { FAST, LIVE, type Timing } from './config.ts';
import { CLAIM_EVENTS, newMessage, QUEUES, SUBSCRIPTIONS, type ClaimInput, type Scenario } from './messages.ts';
import { Orchestrator, type OrchestratorFaults } from './orchestrator/orchestrator.ts';
import { consume } from './runtime.ts';
import { Audit, Notifications, PaymentsService, RepairService, assessmentHandler, fraudHandler, type ServiceFaults } from './services/services.ts';

export { FAST, LIVE };

export type Faults = OrchestratorFaults & ServiceFaults;

// The events the orchestrator reacts to. Its own ClaimApproved/ClaimClosed... go to the same topic for the edges.
const ORCHESTRATOR_EVENTS = new Set([
  'ClaimSubmitted',
  'CancellationRequested',
  'AssessmentPassed',
  'AssessmentFailed',
  'FraudSuspected',
  'FraudCleared',
  'FraudConfirmed',
  'RepairBooked',
  'RepairBookingCancelled',
  'RepairCompleted',
  'PaymentSettled',
]);

/**
 * The reference architecture from the deck, wired together in one process for the demo.
 * Every arrow goes through the broker; nothing calls anything directly.
 *
 *   intake -> [claim-events topic] -> orchestrator --commands--> [queues] -> services --events--> [claim-events]
 *                                  -> notifications (subscription)
 *                                  -> audit (subscription)
 */
export function createSystem(timing: Timing = LIVE) {
  const faults: Faults = {
    outboxEnabled: true,
    crashNextCommit: false,
    repairDown: false,
    repairDelayMs: 0,
    assessmentDelayMs: 0,
    paymentsIdempotent: true,
    relayCrashOnce: false,
  };

  const broker = new Broker(timing.tickMs, timing.lockMs);
  broker.declareTopic(CLAIM_EVENTS);
  for (const q of Object.values(QUEUES)) broker.declareQueue(q);
  broker.subscribe(CLAIM_EVENTS, SUBSCRIPTIONS.orchestrator, (m) => ORCHESTRATOR_EVENTS.has(m.type));
  broker.subscribe(CLAIM_EVENTS, SUBSCRIPTIONS.notifications, (m) => m.type.startsWith('Claim') && m.type !== 'ClaimSubmitted');
  broker.subscribe(CLAIM_EVENTS, SUBSCRIPTIONS.audit);

  const orchestrator = new Orchestrator(broker, { timeouts: timing.timeouts, maxResends: timing.maxResends }, faults, faults);
  const repairs = new RepairService(broker, timing, faults);
  const payments = new PaymentsService(broker, timing, faults);
  const notifications = new Notifications();
  const audit = new Audit();

  consume(broker, SUBSCRIPTIONS.orchestrator, timing, (m) => orchestrator.handle(m));
  consume(broker, QUEUES.timeouts, timing, (m) => orchestrator.handle(m));
  consume(broker, QUEUES.assessment, timing, assessmentHandler(broker, timing, faults));
  consume(broker, QUEUES.fraud, timing, fraudHandler(broker, timing));
  // One queue carries both repair commands; route on type, as a real command endpoint would.
  consume(broker, QUEUES.repairs, timing, (m) => (m.type === 'CancelRepairBooking' ? repairs.cancel(m) : repairs.book(m)));
  consume(broker, QUEUES.payments, timing, payments.settle);
  consume(broker, SUBSCRIPTIONS.notifications, timing, notifications.handle);
  consume(broker, SUBSCRIPTIONS.audit, timing, audit.handle);

  const relays = [orchestrator.outbox, repairs.outbox, payments.outbox];
  let relayTimer: NodeJS.Timeout | undefined;
  let seq = 0;

  const system = {
    timing,
    broker,
    faults,
    orchestrator,
    repairs,
    payments,
    notifications,
    audit,

    start() {
      broker.start();
      relayTimer = setInterval(() => relays.forEach((r) => r.relay()), timing.tickMs * 2);
      return system;
    },
    stop() {
      broker.stop();
      clearInterval(relayTimer);
    },

    /** Claims intake API: accepts a claim and publishes ClaimSubmitted. It owns no state. */
    submit(input: Partial<ClaimInput> = {}): string {
      const scenario: Scenario = { assessment: 'pass', fraud: 'none', ...input.scenario };
      const claimId = `CLM-${String(++seq).padStart(4, '0')}`;
      const body = { customer: input.customer ?? 'A. Customer', description: input.description ?? 'Rear-ended at a junction', amount: input.amount ?? 2400, scenario };
      broker.publish(CLAIM_EVENTS, newMessage('event', 'ClaimSubmitted', body, claimId));
      return claimId;
    },

    /** The customer cancels. The intake API only publishes the fact; the state machine decides what it means. */
    cancel(claimId: string) {
      broker.publish(CLAIM_EVENTS, newMessage('event', 'CancellationRequested', { claimId }, claimId));
    },

    /** Card D: a malformed command lands in the payments queue. */
    injectPoison() {
      broker.send(QUEUES.payments, newMessage('command', 'SettlePayment', { claimId: 'CLM-POISON', amount: 'not-a-number' }, 'CLM-POISON'));
    },

    /** Cards A: the next message of this type is processed, then delivered again (the ack was lost). */
    duplicateNext(endpoint: string, type: string) {
      broker.duplicateNext(endpoint, type);
    },

    /** The numbers behind the dashboard. Queries read state; they never go through the bus. */
    snapshot() {
      const claims = orchestrator.claims().map((c) => ({ ...c, history: orchestrator.history(c.id) }));
      const stats = broker.stats();
      return {
        now: Date.now(),
        faults: { ...faults },
        claims,
        queues: stats,
        deadLetters: stats
          .filter((s) => s.deadLettered)
          .flatMap((s) => broker.deadLetters(s.name).map((d) => ({ endpoint: s.name, id: d.msg.id, type: d.msg.type, claimId: d.msg.correlationId, reason: d.reason, body: d.msg.body, at: d.at }))),
        log: broker.log.slice(-120),
        payments: payments.ledger(),
        bookings: repairs.bookings(),
        notifications: notifications.sent.slice(-30),
        audit: audit.entries.slice(-30),
        outboxPending: relays.map((r) => r.pending()).reduce((a, b) => a + b, 0),
      };
    },
  };
  return system;
}

export type System = ReturnType<typeof createSystem>;
