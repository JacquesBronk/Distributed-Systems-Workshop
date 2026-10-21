import type { State } from './orchestrator/machine.ts';

export interface Timing {
  tickMs: number; // broker pump interval
  lockMs: number; // peek-lock duration: a handler slower than this gets the message redelivered
  latency: [number, number]; // simulated service work
  immediateRetries: number; // "a blip": retry straight away
  backoffMs: number[]; // "an outage": delayed redelivery, last value repeats
  maxDeliveries: number; // "a poison message": then dead-letter
  timeouts: Partial<Record<State, number>>; // "silence": the state machine's deadlines
  maxResends: number; // timeouts resend the command this often before escalating
  repairDurationMs: number;
  fraudReviewMs: number;
  breakerThreshold: number; // outbound connectors: consecutive failures before the circuit opens
  breakerCooldownMs: number; // ...and how long before one probe call is allowed
}

export const LIVE: Timing = {
  tickMs: 25,
  lockMs: 15_000,
  latency: [600, 1400],
  immediateRetries: 2,
  backoffMs: [1000, 2000, 4000],
  maxDeliveries: 6,
  timeouts: {
    Assessing: 20_000,
    FraudReview: 30_000,
    Approved: 20_000,
    RepairScheduled: 60_000,
    Paying: 20_000,
    Cancelling: 20_000,
  },
  maxResends: 2,
  repairDurationMs: 6000,
  fraudReviewMs: 5000,
  breakerThreshold: 3,
  breakerCooldownMs: 5000,
};

// Same behaviour, compressed so the test suite runs in seconds.
export const FAST: Timing = {
  tickMs: 2,
  lockMs: 300,
  latency: [1, 4],
  immediateRetries: 2,
  backoffMs: [8, 16, 32],
  maxDeliveries: 6,
  timeouts: {
    Assessing: 400,
    FraudReview: 600,
    Approved: 400,
    RepairScheduled: 800,
    Paying: 400,
    Cancelling: 400,
  },
  maxResends: 2,
  repairDurationMs: 60,
  fraudReviewMs: 40,
  breakerThreshold: 3,
  breakerCooldownMs: 30,
};
