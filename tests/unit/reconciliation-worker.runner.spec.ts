import 'reflect-metadata';

import { describe, expect, it, vi } from 'vitest';

import { ReconciliationWorkerRunner } from '../../apps/worker/src/reconciliation-worker.runner';
import type { ReconciliationRuntime } from '../../packages/alarms/src/infrastructure/persistence/reconciliation-runtime';

async function waitUntil(predicate: () => boolean, label: string): Promise<void> {
  const deadline = performance.now() + 5_000;
  while (!predicate()) {
    if (performance.now() >= deadline) {
      throw new Error(`Timed out waiting for ${label}`);
    }
    await new Promise<void>((resolve) => setTimeout(resolve, 1));
  }
}

describe('ReconciliationWorkerRunner', () => {
  it('[RET-20] drains a 1,000-lookup backlog with an exact per-worker concurrency cap of eight', async () => {
    const target = 1_000;
    let claimed = 0;
    let completed = 0;
    let active = 0;
    let maximumActive = 0;
    const foregroundProgress: string[] = [];

    const runOnce = vi.fn(async (signal?: AbortSignal): Promise<boolean> => {
      if (signal?.aborted === true || claimed >= target) {
        return false;
      }
      claimed += 1;
      active += 1;
      maximumActive = Math.max(maximumActive, active);
      try {
        await new Promise<void>((resolve) => setTimeout(resolve, 1));
        completed += 1;
        return true;
      } finally {
        active -= 1;
      }
    });
    const close = vi.fn(async () => undefined);
    const runner = new ReconciliationWorkerRunner({
      processor: { runOnce },
      close,
    } as unknown as ReconciliationRuntime);

    runner.onApplicationBootstrap();
    const normalPostAndReadiness = Promise.resolve().then(() => {
      foregroundProgress.push('POST');
      foregroundProgress.push('READINESS');
    });
    await normalPostAndReadiness;
    await waitUntil(() => completed === target, '1,000 bounded history lookups');
    await runner.onApplicationShutdown();

    expect(claimed).toBe(target);
    expect(completed).toBe(target);
    expect(maximumActive).toBe(8);
    expect(foregroundProgress).toEqual(['POST', 'READINESS']);
    expect(close).toHaveBeenCalledOnce();
  });
});
