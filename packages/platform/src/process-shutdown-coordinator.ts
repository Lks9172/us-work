export type NodeShutdownSignal = 'SIGINT' | 'SIGTERM';

export interface NodeSignalSource {
  on(signal: NodeShutdownSignal, listener: () => void): unknown;
  removeListener(signal: NodeShutdownSignal, listener: () => void): unknown;
}

export interface NodeProcessShutdownCoordinatorOptions {
  readonly signalSource?: NodeSignalSource;
  readonly monotonicNow?: () => number;
  readonly onShutdownRequested?: (signal: NodeShutdownSignal) => void;
  readonly onShutdownCompleted?: (durationMs: number) => void;
  readonly onShutdownFailed?: (error: unknown) => void;
}

type CloseApplication = () => void | PromiseLike<void>;

/**
 * Coordinates SIGINT/SIGTERM with an application that is created asynchronously.
 *
 * Signal listeners are registered in the constructor, so callers can construct the
 * coordinator before their first bootstrap await. The first signal is retained until
 * a close handler is attached; later signals are coalesced into that same close.
 */
export class NodeProcessShutdownCoordinator {
  private readonly signalSource: NodeSignalSource;
  private readonly monotonicNow: () => number;
  private readonly onShutdownRequested: (signal: NodeShutdownSignal) => void;
  private readonly onShutdownCompleted: (durationMs: number) => void;
  private readonly onShutdownFailed: (error: unknown) => void;
  private readonly sigintListener = (): void => this.requestShutdown('SIGINT');
  private readonly sigtermListener = (): void => this.requestShutdown('SIGTERM');
  private readonly completion: Promise<void>;
  private resolveCompletion: (() => void) | null = null;
  private requestedSignal: NodeShutdownSignal | null = null;
  private requestedAtMs = 0;
  private closeApplication: CloseApplication | null = null;
  private shutdownPromise: Promise<void> | null = null;
  private disposed = false;

  public constructor(options: NodeProcessShutdownCoordinatorOptions = {}) {
    this.signalSource = options.signalSource ?? process;
    this.monotonicNow = options.monotonicNow ?? (() => performance.now());
    this.onShutdownRequested = options.onShutdownRequested ?? (() => undefined);
    this.onShutdownCompleted = options.onShutdownCompleted ?? (() => undefined);
    this.onShutdownFailed = options.onShutdownFailed ?? (() => undefined);
    this.completion = new Promise((resolve) => {
      this.resolveCompletion = resolve;
    });

    this.signalSource.on('SIGINT', this.sigintListener);
    this.signalSource.on('SIGTERM', this.sigtermListener);
  }

  public get shutdownRequested(): boolean {
    return this.requestedSignal !== null;
  }

  public setCloseHandler(closeApplication: CloseApplication): void {
    if (this.disposed) {
      throw new Error('Cannot attach a close handler after the shutdown coordinator is disposed');
    }
    if (this.closeApplication !== null) {
      throw new Error('A close handler is already attached');
    }

    this.closeApplication = closeApplication;
    this.beginShutdownIfReady();
  }

  public waitForShutdown(): Promise<void> {
    return this.completion;
  }

  public dispose(): void {
    if (this.disposed) {
      return;
    }
    this.disposed = true;
    this.signalSource.removeListener('SIGINT', this.sigintListener);
    this.signalSource.removeListener('SIGTERM', this.sigtermListener);
    this.resolveCompletion?.();
    this.resolveCompletion = null;
  }

  private requestShutdown(signal: NodeShutdownSignal): void {
    if (this.disposed || this.requestedSignal !== null) {
      return;
    }

    this.requestedSignal = signal;
    this.requestedAtMs = this.readMonotonicNow();
    this.invokeSafely(() => this.onShutdownRequested(signal));
    this.beginShutdownIfReady();
  }

  private beginShutdownIfReady(): void {
    if (
      this.requestedSignal === null ||
      this.closeApplication === null ||
      this.shutdownPromise !== null ||
      this.disposed
    ) {
      return;
    }

    const closeApplication = this.closeApplication;
    this.shutdownPromise = (async () => {
      try {
        await closeApplication();
        const durationMs = Math.max(0, this.readMonotonicNow() - this.requestedAtMs);
        this.invokeSafely(() => this.onShutdownCompleted(durationMs));
      } catch (error: unknown) {
        this.invokeSafely(() => this.onShutdownFailed(error));
      } finally {
        this.dispose();
      }
    })();
  }

  private readMonotonicNow(): number {
    try {
      const value = this.monotonicNow();
      return Number.isFinite(value) ? value : 0;
    } catch {
      return 0;
    }
  }

  private invokeSafely(callback: () => void): void {
    try {
      callback();
    } catch {
      // Observability callbacks must not prevent application shutdown.
    }
  }
}
