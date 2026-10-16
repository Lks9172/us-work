import { EventEmitter } from 'node:events';

import { describe, expect, it, vi } from 'vitest';

import {
  NodeProcessShutdownCoordinator,
  type NodeShutdownSignal,
} from '../../packages/platform/src/process-shutdown-coordinator';

class FakeSignalSource extends EventEmitter {
  public emitSignal(signal: NodeShutdownSignal): void {
    this.emit(signal);
  }
}

describe('NodeProcessShutdownCoordinator', () => {
  it('defers an early signal until the application exists and coalesces close exactly once', async () => {
    const signalSource = new FakeSignalSource();
    const events: string[] = [];
    let now = 10;
    const closeApplication = vi.fn(async () => {
      events.push('close');
      now = 25;
    });
    const coordinator = new NodeProcessShutdownCoordinator({
      signalSource,
      monotonicNow: () => now,
      onShutdownRequested: (signal) => events.push(`requested:${signal}`),
      onShutdownCompleted: (durationMs) => events.push(`stopped:${durationMs}`),
    });

    signalSource.emitSignal('SIGTERM');
    signalSource.emitSignal('SIGTERM');
    signalSource.emitSignal('SIGINT');
    expect(closeApplication).not.toHaveBeenCalled();

    coordinator.setCloseHandler(closeApplication);
    signalSource.emitSignal('SIGTERM');
    await coordinator.waitForShutdown();

    expect(closeApplication).toHaveBeenCalledTimes(1);
    expect(events).toEqual(['requested:SIGTERM', 'close', 'stopped:15']);
    expect(signalSource.listenerCount('SIGINT')).toBe(0);
    expect(signalSource.listenerCount('SIGTERM')).toBe(0);
  });

  it('closes once when the application exists before either signal arrives', async () => {
    const signalSource = new FakeSignalSource();
    const closeApplication = vi.fn(async () => undefined);
    const coordinator = new NodeProcessShutdownCoordinator({ signalSource });
    coordinator.setCloseHandler(closeApplication);

    signalSource.emitSignal('SIGINT');
    signalSource.emitSignal('SIGTERM');
    await coordinator.waitForShutdown();

    expect(coordinator.shutdownRequested).toBe(true);
    expect(closeApplication).toHaveBeenCalledTimes(1);
  });

  it('contains close and observability failures, reports failure, and removes listeners', async () => {
    const signalSource = new FakeSignalSource();
    const closeError = new Error('private shutdown detail');
    const failures: unknown[] = [];
    const coordinator = new NodeProcessShutdownCoordinator({
      signalSource,
      onShutdownRequested: () => {
        throw new Error('logger unavailable');
      },
      onShutdownCompleted: () => {
        throw new Error('must not run');
      },
      onShutdownFailed: (error) => {
        failures.push(error);
        throw new Error('logger unavailable');
      },
    });
    coordinator.setCloseHandler(() => {
      throw closeError;
    });

    signalSource.emitSignal('SIGTERM');
    await expect(coordinator.waitForShutdown()).resolves.toBeUndefined();

    expect(failures).toEqual([closeError]);
    expect(signalSource.listenerCount('SIGINT')).toBe(0);
    expect(signalSource.listenerCount('SIGTERM')).toBe(0);
  });

  it('cleans up both signal listeners when bootstrap fails before an application exists', async () => {
    const signalSource = new FakeSignalSource();
    const closeApplication = vi.fn();
    const coordinator = new NodeProcessShutdownCoordinator({ signalSource });

    coordinator.dispose();
    signalSource.emitSignal('SIGTERM');
    await coordinator.waitForShutdown();

    expect(coordinator.shutdownRequested).toBe(false);
    expect(closeApplication).not.toHaveBeenCalled();
    expect(signalSource.listenerCount('SIGINT')).toBe(0);
    expect(signalSource.listenerCount('SIGTERM')).toBe(0);
  });
});
