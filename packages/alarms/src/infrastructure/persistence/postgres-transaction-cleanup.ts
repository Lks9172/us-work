import type { PoolClient } from 'pg';

export const POSTGRES_ROLLBACK_TIMEOUT_MS = 5_000;

export async function rollbackPostgresTransaction(client: PoolClient): Promise<void> {
  let timeoutHandle: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<void>((resolve) => {
    timeoutHandle = setTimeout(resolve, POSTGRES_ROLLBACK_TIMEOUT_MS);
    timeoutHandle.unref();
  });
  const rollback = Promise.resolve()
    .then(() => client.query('ROLLBACK'))
    .then(
      () => undefined,
      () => undefined,
    );

  try {
    await Promise.race([rollback, timeout]);
  } finally {
    if (timeoutHandle !== undefined) {
      clearTimeout(timeoutHandle);
    }
  }
}
