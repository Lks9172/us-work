import { randomUUID } from 'node:crypto';

import type {
  DispatchHeartbeatHandle,
  DispatchHeartbeatScheduler,
} from '../../application/dispatch';
import { ExpansionAbortedError } from '../../application/expansion';

export function nodeGenerateId(): string {
  return randomUUID();
}

export function nodeMonotonicNow(): number {
  return performance.now();
}

export function nodeWaitUntil(targetMonotonicMs: number, signal?: AbortSignal): Promise<void> {
  const remaining = Math.max(0, targetMonotonicMs - performance.now());
  if (signal?.aborted === true) {
    return Promise.resolve();
  }

  return new Promise((resolve) => {
    let timer: ReturnType<typeof setTimeout>;
    const finish = (): void => {
      clearTimeout(timer);
      signal?.removeEventListener('abort', finish);
      resolve();
    };
    timer = setTimeout(finish, remaining);
    signal?.addEventListener('abort', finish, { once: true });
  });
}

export function nodeExpansionSleep(milliseconds: number, signal?: AbortSignal): Promise<void> {
  if (signal?.aborted === true) {
    return Promise.reject(new ExpansionAbortedError());
  }

  return new Promise((resolve, reject) => {
    let timer: ReturnType<typeof setTimeout>;
    const abort = (): void => {
      clearTimeout(timer);
      signal?.removeEventListener('abort', abort);
      reject(new ExpansionAbortedError());
    };
    const complete = (): void => {
      signal?.removeEventListener('abort', abort);
      resolve();
    };
    timer = setTimeout(complete, milliseconds);
    signal?.addEventListener('abort', abort, { once: true });
  });
}

export function nodeRandom(): number {
  return Math.random();
}

export class NodeDispatchHeartbeatScheduler implements DispatchHeartbeatScheduler {
  public start(intervalMs: number, heartbeat: () => Promise<boolean>): DispatchHeartbeatHandle {
    let stopped = false;
    let inFlight = Promise.resolve();
    const timer = setInterval(() => {
      if (stopped) {
        return;
      }
      inFlight = inFlight
        .then(async () => {
          if (!stopped && !(await heartbeat())) {
            stopped = true;
            clearInterval(timer);
          }
        })
        .catch(() => {
          stopped = true;
          clearInterval(timer);
        });
    }, intervalMs);
    timer.unref();

    return {
      stop: async () => {
        stopped = true;
        clearInterval(timer);
        await inFlight;
      },
    };
  }
}
