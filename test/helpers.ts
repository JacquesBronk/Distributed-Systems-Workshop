import { createSystem, FAST, type System } from '../src/system.ts';

export async function until(cond: () => boolean, what: string, timeoutMs = 4000) {
  const start = Date.now();
  while (!cond()) {
    if (Date.now() - start > timeoutMs) throw new Error(`timed out waiting for: ${what}`);
    await new Promise((r) => setTimeout(r, 3));
  }
}

export const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

export function boot(): System {
  return createSystem(FAST).start();
}

export const stateOf = (s: System, id: string) => s.orchestrator.claim(id)?.state;
export const untilState = (s: System, id: string, state: string, timeoutMs?: number) =>
  until(() => stateOf(s, id) === state, `${id} to reach ${state} (is ${stateOf(s, id)})`, timeoutMs);

export const path = (s: System, id: string) => s.orchestrator.history(id).filter((h) => !h.note?.startsWith('ignored')).map((h) => h.to);
export const ignored = (s: System, id: string, event?: string) =>
  s.orchestrator.history(id).filter((h) => h.note?.startsWith('ignored') && (!event || h.event === event));
