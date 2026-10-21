import { Broker } from './broker.ts';
import { FAST, LIVE, type Timing } from './config.ts';
import { CLAIM_EVENTS, newMessage, QUEUES, SUBSCRIPTIONS, type ClaimInput, type Scenario } from './messages.ts';
import { Orchestrator, type OrchestratorFaults } from './orchestrator/orchestrator.ts';
import { consume } from './runtime.ts';
import { ExternalProvider, GarageNetwork, type GarageFaults } from './connectors/external.ts';
import { ConnectorLog, GarageInbound } from './connectors/inbound.ts';
import { OutboundConnector, type Rule } from './connectors/outbound.ts';
import { Audit, Notifications, PaymentsService, RepairService, assessmentHandler, fraudHandler, type ServiceFaults } from './services/services.ts';

export { FAST, LIVE };

export type Faults = OrchestratorFaults & ServiceFaults & GarageFaults & { garageDown: boolean; reinsurerDown: boolean };

const GARAGE_WEBHOOK_SECRET = 'whsec_demo_garage';

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
    garageDown: false,
    reinsurerDown: false,
    garageDuplicateWebhooks: false,
    garageCorruptSignatureOnce: false,
  };

  const broker = new Broker(timing.tickMs, timing.lockMs);
  broker.declareTopic(CLAIM_EVENTS);
  for (const q of Object.values(QUEUES)) broker.declareQueue(q);
  broker.subscribe(CLAIM_EVENTS, SUBSCRIPTIONS.orchestrator, (m) => ORCHESTRATOR_EVENTS.has(m.type));
  broker.subscribe(CLAIM_EVENTS, SUBSCRIPTIONS.notifications, (m) => m.type.startsWith('Claim') && m.type !== 'ClaimSubmitted');
  broker.subscribe(CLAIM_EVENTS, SUBSCRIPTIONS.audit);
  broker.subscribe(CLAIM_EVENTS, SUBSCRIPTIONS.garageDispatch, (m) => m.type === 'RepairBooked' || m.type === 'RepairBookingCancelled');
  broker.subscribe(CLAIM_EVENTS, SUBSCRIPTIONS.partnerReporting, (m) => ['ClaimClosed', 'ClaimRejected', 'ClaimCancelled'].includes(m.type));

  const orchestrator = new Orchestrator(broker, { timeouts: timing.timeouts, maxResends: timing.maxResends }, faults, faults);
  const repairs = new RepairService(broker, timing, faults);
  const payments = new PaymentsService(broker, timing, faults);
  const notifications = new Notifications();
  const audit = new Audit();

  // --- the edge: connectors to systems we don't own ---
  const connectorLog = new ConnectorLog();
  const garageInbound = new GarageInbound(broker, GARAGE_WEBHOOK_SECRET, connectorLog, faults);
  const garage = new GarageNetwork({
    secret: GARAGE_WEBHOOK_SECRET,
    isDown: () => faults.garageDown,
    faults,
    sink: (raw, headers) => garageInbound.handle(raw, headers),
    latency: timing.latency,
    repairMs: timing.repairDurationMs,
  });
  const reinsurer = new ExternalProvider('reinsurer', () => faults.reinsurerDown, timing.latency);
  const fraudBureau = new ExternalProvider('fraud-bureau', () => false, timing.latency);
  const providers = { garage, reinsurer, 'fraud-bureau': fraudBureau };

  const REPORTING_THRESHOLD = 5000;
  const dispatchRules: Rule[] = [
    {
      name: 'dispatch-job',
      action: 'send',
      when: (m) => m.type === 'RepairBooked',
      target: 'garage',
      path: '/jobs',
      idempotencyKey: (m) => `job:${m.correlationId}`, // one job per claim, however many times we ask
      transform: (m) => ({ client_reference: m.correlationId, job_type: 'accident-repair', description: m.body.description, customer_name: m.body.customer }),
      after: (m) => garageInbound.recordDispatched(m.correlationId),
    },
    {
      name: 'cancel-job',
      action: 'send',
      when: (m) => m.type === 'RepairBookingCancelled',
      target: 'garage',
      path: '/jobs/cancel',
      idempotencyKey: (m) => `cancel:${m.correlationId}`,
      transform: (m) => ({ client_reference: m.correlationId }),
    },
  ];
  const reportingRules: Rule[] = [
    {
      name: 'fraud-to-bureau',
      action: 'send',
      when: (m) => m.type === 'ClaimRejected' && m.body.reason === 'fraud confirmed',
      target: 'fraud-bureau',
      path: '/cases',
      idempotencyKey: (m) => `fraud:${m.correlationId}`,
      transform: (m) => ({ case_ref: m.correlationId, reason: m.body.reason, claimed_amount_gbp: m.body.amount }),
    },
    {
      name: 'large-claims-to-reinsurer',
      action: 'send',
      when: (m) => m.type === 'ClaimClosed' && m.body.amount >= REPORTING_THRESHOLD,
      target: 'reinsurer',
      path: '/bordereau',
      idempotencyKey: (m) => `closed:${m.correlationId}`,
      transform: (m) => ({ claim_ref: m.correlationId, paid_pence: Math.round(m.body.amount * 100), currency: 'GBP', closed_at: new Date(m.at).toISOString() }),
    },
    { name: 'small-claims', action: 'skip', when: (m) => m.type === 'ClaimClosed', reason: `below the £${REPORTING_THRESHOLD} reporting threshold` },
    { name: 'not-reportable', action: 'skip', when: (m) => m.type === 'ClaimCancelled', reason: 'cancelled claims are not reported' },
  ];
  const garageDispatch = new OutboundConnector('garage-dispatch', dispatchRules, providers, connectorLog, timing);
  const partnerReporting = new OutboundConnector('partner-reporting', reportingRules, providers, connectorLog, timing);

  consume(broker, SUBSCRIPTIONS.orchestrator, timing, (m) => orchestrator.handle(m));
  consume(broker, QUEUES.timeouts, timing, (m) => orchestrator.handle(m));
  consume(broker, QUEUES.assessment, timing, assessmentHandler(broker, timing, faults));
  consume(broker, QUEUES.fraud, timing, fraudHandler(broker, timing));
  // One queue carries both repair commands; route on type, as a real command endpoint would.
  consume(broker, QUEUES.repairs, timing, (m) => (m.type === 'CancelRepairBooking' ? repairs.cancel(m) : repairs.book(m)));
  consume(broker, QUEUES.payments, timing, payments.settle);
  consume(broker, SUBSCRIPTIONS.notifications, timing, notifications.handle);
  consume(broker, SUBSCRIPTIONS.audit, timing, audit.handle);
  consume(broker, SUBSCRIPTIONS.garageDispatch, timing, garageDispatch.handle);
  consume(broker, SUBSCRIPTIONS.partnerReporting, timing, partnerReporting.handle);

  const relays = [orchestrator.outbox, repairs.outbox, payments.outbox, garageInbound.outbox];
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
    garage,
    garageInbound,
    reinsurer,
    fraudBureau,

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

    /** The garage's webhook endpoint (the HTTP route in server.ts calls this). */
    receiveGarageWebhook(rawBody: string, headers: Record<string, string | undefined>) {
      return garageInbound.handle(rawBody, headers);
    },

    /** Webhooks a provider should never send: for a job we never dispatched, or with a broken body. */
    sendStrayWebhook(kind: 'unknown-job' | 'malformed') {
      return kind === 'unknown-job' ? garage.webhook({ type: 'job.completed', client_reference: 'CLM-9999' }) : garage.malformedWebhook();
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
        connectors: {
          log: connectorLog.entries.slice(-40),
          providers: [garage, reinsurer, fraudBureau].map((p) => ({ ...p.stats(), recent: p.received.slice(-4) })),
          circuits: [...garageDispatch.circuits(), ...partnerReporting.circuits()],
          quarantine: garageInbound.quarantined(),
        },
        outboxPending: relays.map((r) => r.pending()).reduce((a, b) => a + b, 0),
      };
    },
  };
  return system;
}

export type System = ReturnType<typeof createSystem>;
