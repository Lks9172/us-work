import { readFile } from 'node:fs/promises';

import { describe, expect, it } from 'vitest';

const composeFile = new URL('../../docker-compose.yml', import.meta.url);
const workflowFile = new URL('../../.github/workflows/verify.yml', import.meta.url);
const limiterFile = new URL(
  '../../packages/alarms/src/infrastructure/limiter/redis-paced-rate-limiter.ts',
  import.meta.url,
);

describe('Compose runtime contract', () => {
  it('uses the required mock image and PostgreSQL 18 volume root', async () => {
    const compose = await readFile(composeFile, 'utf8');

    expect(compose).toContain('ghcr.io/us-all/backend-assignment-api:1.2');
    expect(compose).toContain('postgres-data:/var/lib/postgresql');
    expect(compose).not.toContain('postgres-data:/var/lib/postgresql/data');
  });

  it('does not assign one identity to every scaled worker', async () => {
    const compose = await readFile(composeFile, 'utf8');

    expect(compose).not.toContain('WORKER_ID:');
    expect(compose).toContain('replicas: 3');
  });

  it('restarts failed API and worker processes without restarting the migration job', async () => {
    const compose = await readFile(composeFile, 'utf8');

    expect(compose.match(/restart: unless-stopped/g)).toHaveLength(2);
    expect(compose).toContain('restart: "no"');
  });

  it('gives committed provider requests longer than the response timeout to drain', async () => {
    const compose = await readFile(composeFile, 'utf8');

    expect(compose).toContain('stop_grace_period: 45s');
  });

  it('keeps Redis limiter state on a no-eviction instance', async () => {
    const compose = await readFile(composeFile, 'utf8');

    expect(compose).toContain(
      'redis-server", "--appendonly", "yes", "--maxmemory-policy", "noeviction"',
    );
  });

  it('co-locates limiter authority and audit keys and uses caller-generated UUID reservations', async () => {
    const limiter = await readFile(limiterFile, 'utf8');
    const interpolatedKeyPrefix = '$' + '{keyPrefix}';

    expect(limiter).toContain("const DEFAULT_KEY_PREFIX = 'usall'");
    expect(limiter).toContain(`\`${interpolatedKeyPrefix}:limiter:{default-provider}\``);
    expect(limiter).toContain(`\`${interpolatedKeyPrefix}:limiter:audit:{default-provider}\``);
    expect(limiter).toContain('const reservationId = randomUUID()');
    expect(limiter).toContain('keys: [this.limiterKey, this.limiterAuditKey]');
  });

  it('blocks both API and worker startup until the one-shot migration succeeds', async () => {
    const compose = await readFile(composeFile, 'utf8');
    const completedMigrationDependencies = compose.match(
      /migrate:\s*\n\s*condition: service_completed_successfully/g,
    );

    expect(completedMigrationDependencies).toHaveLength(2);
    expect(compose).toContain('restart: "no"');
  });

  it('publishes only the API and binds it to localhost', async () => {
    const compose = await readFile(composeFile, 'utf8');

    expect(compose.match(/^\s+ports:\s*$/gm)).toHaveLength(1);
    expect(compose).toContain('127.0.0.1:$' + '{API_PORT:-3000}:3000');
  });

  it('smoke-tests the dependency-backed runtime instead of starting entrypoints without config', async () => {
    const workflow = await readFile(workflowFile, 'utf8');

    expect(workflow).toContain('docker compose -p usall-ci-smoke up --detach --wait');
    expect(workflow).toContain('http://127.0.0.1:3000/health/ready');
    expect(workflow).toContain('label=com.docker.compose.service=worker');
    expect(workflow).not.toContain('docker run --detach --name usall-api-smoke');
  });
});
