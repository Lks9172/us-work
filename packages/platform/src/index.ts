export { PROCESS_INSTANCE_ID } from './process-identity';
export {
  NodeProcessShutdownCoordinator,
  type NodeProcessShutdownCoordinatorOptions,
  type NodeShutdownSignal,
  type NodeSignalSource,
} from './process-shutdown-coordinator';
export { type SafeErrorMetadata, safeErrorMetadata } from './safe-error';

/**
 * Resolve after a delay, or immediately when the supplied signal is aborted.
 * The abort listener is removed on both paths so a long-lived worker signal does
 * not retain one listener for every polling iteration.
 */
export function abortableDelay(milliseconds: number, signal?: AbortSignal): Promise<void> {
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

    timer = setTimeout(finish, milliseconds);
    signal?.addEventListener('abort', finish, { once: true });
  });
}

/**
 * Return a bounded exponential retry delay for a one-based consecutive failure count.
 * Keeping this calculation in the platform package gives every worker loop the same
 * business-neutral outage behaviour without coupling it to an Alarm adapter.
 */
export function boundedExponentialBackoffMs(
  consecutiveFailures: number,
  baseMs = 250,
  capMs = 5_000,
): number {
  if (!Number.isSafeInteger(consecutiveFailures) || consecutiveFailures < 1) {
    throw new RangeError('consecutiveFailures must be a positive safe integer');
  }
  if (!Number.isSafeInteger(baseMs) || baseMs < 1) {
    throw new RangeError('baseMs must be a positive safe integer');
  }
  if (!Number.isSafeInteger(capMs) || capMs < baseMs) {
    throw new RangeError('capMs must be a safe integer greater than or equal to baseMs');
  }

  return Math.min(capMs, baseMs * 2 ** Math.min(consecutiveFailures - 1, 30));
}

export function requireEnvironmentString(value: string | undefined, name: string): string {
  if (value === undefined || value.trim().length === 0) {
    throw new Error(`${name} is required`);
  }
  return value;
}

export function requireEnvironmentUrl(
  value: string | undefined,
  name: string,
  allowedProtocols: readonly string[],
): string {
  const raw = requireEnvironmentString(value, name);
  let parsed: URL;
  try {
    parsed = new URL(raw);
  } catch {
    throw new Error(`${name} must be a valid URL`);
  }
  if (!allowedProtocols.includes(parsed.protocol) || parsed.hostname.length === 0) {
    throw new Error(`${name} must use ${allowedProtocols.join(' or ')} with a host`);
  }
  return raw;
}

export function parseTcpPort(value: string | undefined, fallback: number): number {
  const raw = value ?? String(fallback);
  if (!/^\d+$/.test(raw)) {
    throw new Error('PORT must be an integer between 1 and 65535');
  }
  const port = Number(raw);
  if (!Number.isSafeInteger(port) || port < 1 || port > 65_535) {
    throw new Error('PORT must be an integer between 1 and 65535');
  }
  return port;
}
