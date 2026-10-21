import { randomUUID } from 'node:crypto';

// The three kinds of message from the deck. Queries never touch the bus: they are HTTP reads.
export type Kind = 'command' | 'event';

export interface Message<T = any> {
  id: string; // set once by the publisher; stays stable across redeliveries
  kind: Kind;
  type: string; // imperative for commands (BookRepair), past tense for events (RepairBooked)
  correlationId: string; // the ClaimId: finds the state machine instance and joins up logs
  causationId?: string; // the message that caused this one
  body: T;
  at: number;
}

export function newMessage<T>(
  kind: Kind,
  type: string,
  body: T,
  correlationId: string,
  causationId?: string,
): Message<T> {
  return { id: randomUUID(), kind, type, correlationId, causationId, body, at: Date.now() };
}

export const CLAIM_EVENTS = 'claim-events';

// Queues (commands, one handler each) and the subscriptions on the claim-events topic.
export const QUEUES = {
  assessment: 'assessment',
  fraud: 'fraud-check',
  repairs: 'repair-booking',
  payments: 'payments',
  timeouts: 'orchestrator-timeouts',
} as const;

export const SUBSCRIPTIONS = {
  orchestrator: 'orchestrator',
  notifications: 'notifications',
  audit: 'audit',
  garageDispatch: 'garage-dispatch',
  partnerReporting: 'partner-reporting',
} as const;

export interface ClaimInput {
  customer: string;
  description: string;
  amount: number;
  scenario?: Scenario;
}

// Simulation switches so a facilitator can steer each claim down a particular path.
export interface Scenario {
  assessment: 'pass' | 'fail';
  fraud: 'none' | 'review-clear' | 'review-confirm';
}

export const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));
export const rand = (lo: number, hi: number) => lo + Math.random() * (hi - lo);
