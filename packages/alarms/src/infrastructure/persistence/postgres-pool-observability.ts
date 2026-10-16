import { PROCESS_INSTANCE_ID, safeErrorMetadata } from '@usall-backend-assignment/platform';
import type { Pool, PoolClient } from 'pg';

export interface PostgresPoolErrorHandlerOptions {
  readonly component: string;
  readonly processInstanceId?: string;
  readonly now?: () => Date;
  readonly write?: (line: string) => void;
}

function errorRecord(error: Error, options: PostgresPoolErrorHandlerOptions, eventName: string) {
  return {
    timestamp: (options.now ?? (() => new Date()))().toISOString(),
    level: 'error',
    component: options.component,
    processInstanceId: options.processInstanceId ?? PROCESS_INSTANCE_ID,
    eventName,
    ...safeErrorMetadata(error),
  };
}

function writeError(
  error: Error,
  options: PostgresPoolErrorHandlerOptions,
  eventName: string,
): void {
  const write = options.write ?? ((line: string) => process.stderr.write(line));
  write(`${JSON.stringify(errorRecord(error, options, eventName))}\n`);
}

export function attachPostgresPoolErrorHandler(
  pool: Pick<Pool, 'on'>,
  options: PostgresPoolErrorHandlerOptions,
): void {
  pool.on('error', (error: Error) => {
    writeError(error, options, 'POSTGRES_IDLE_CLIENT_ERROR');
  });
}

export function attachPostgresClientErrorHandler(
  client: Pick<PoolClient, 'on' | 'removeListener'>,
  options: PostgresPoolErrorHandlerOptions,
): () => void {
  const listener = (error: Error): void => {
    writeError(error, options, 'POSTGRES_ACTIVE_CLIENT_ERROR');
  };
  client.on('error', listener);
  return () => client.removeListener('error', listener);
}
