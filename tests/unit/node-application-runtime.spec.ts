import { describe, expect, it, vi } from 'vitest';

import { NodeDispatchHeartbeatScheduler } from '../../packages/alarms/src/infrastructure/runtime/node-application-runtime';

describe('NodeDispatchHeartbeatScheduler', () => {
  it('LF-05 never overlaps a slow heartbeat and drops queued ticks after stop', async () => {
    vi.useFakeTimers();
    try {
      let active = 0;
      let maximumActive = 0;
      let calls = 0;
      let release: (() => void) | undefined;
      const firstHeartbeat = new Promise<void>((resolve) => {
        release = resolve;
      });
      const scheduler = new NodeDispatchHeartbeatScheduler();
      const handle = scheduler.start(10, async () => {
        calls += 1;
        active += 1;
        maximumActive = Math.max(maximumActive, active);
        await firstHeartbeat;
        active -= 1;
        return true;
      });

      await vi.advanceTimersByTimeAsync(50);
      expect(calls).toBe(1);
      expect(maximumActive).toBe(1);

      const stopped = handle.stop();
      release?.();
      await stopped;
      await vi.advanceTimersByTimeAsync(50);

      expect(calls).toBe(1);
      expect(active).toBe(0);
      expect(maximumActive).toBe(1);
    } finally {
      vi.useRealTimers();
    }
  });

  it('LF-05 stops scheduling immediately when the durable heartbeat fence is lost', async () => {
    vi.useFakeTimers();
    try {
      let calls = 0;
      const scheduler = new NodeDispatchHeartbeatScheduler();
      const handle = scheduler.start(10, async () => {
        calls += 1;
        return false;
      });

      await vi.advanceTimersByTimeAsync(100);
      expect(calls).toBe(1);
      await handle.stop();
    } finally {
      vi.useRealTimers();
    }
  });
});
