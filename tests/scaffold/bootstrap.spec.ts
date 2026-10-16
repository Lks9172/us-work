import { readdir, readFile } from 'node:fs/promises';

import { MODULE_METADATA } from '@nestjs/common/constants';
import { describe, expect, it } from 'vitest';

import { ApiModule } from '../../apps/api/src/api.module';
import { DocsController } from '../../apps/api/src/docs.controller';
import { authoritativeEmptyGraceMs, WorkerModule } from '../../apps/worker/src/worker.module';

describe('application bootstrap scaffold', () => {
  it('keeps the worker free of HTTP controllers', () => {
    const controllers = Reflect.getMetadata(MODULE_METADATA.CONTROLLERS, WorkerModule) ?? [];

    expect(controllers).toEqual([]);
  });

  it('exposes Alarm HTTP routes only from the API process', () => {
    const controllers = Reflect.getMetadata(MODULE_METADATA.CONTROLLERS, ApiModule) ?? [];

    expect(controllers.map((controller: { name: string }) => controller.name)).toEqual([
      'AlarmsController',
      'HealthController',
      'DocsController',
    ]);
    expect(controllers).toContain(DocsController);
  });

  it('uses separate HTTP and application-context bootstraps', async () => {
    const [apiSource, workerSource] = await Promise.all([
      readFile(new URL('../../apps/api/src/main.ts', import.meta.url), 'utf8'),
      readFile(new URL('../../apps/worker/src/main.ts', import.meta.url), 'utf8'),
    ]);

    expect(apiSource).toContain('NestFactory.create<NestFastifyApplication>');
    expect(workerSource).toContain('NestFactory.createApplicationContext');
    expect(workerSource).not.toContain('.listen(');
  });

  it('installs coordinated signal handling before the first bootstrap await', async () => {
    const sources = await Promise.all([
      readFile(new URL('../../apps/api/src/main.ts', import.meta.url), 'utf8'),
      readFile(new URL('../../apps/worker/src/main.ts', import.meta.url), 'utf8'),
    ]);

    for (const source of sources) {
      const coordinator = source.indexOf('new NodeProcessShutdownCoordinator');
      const firstAwait = source.indexOf('await ');
      expect(coordinator).toBeGreaterThanOrEqual(0);
      expect(firstAwait).toBeGreaterThan(coordinator);
      expect(source).toContain('shutdownCoordinator.setCloseHandler');
      expect(source).toContain('shutdownCoordinator.shutdownRequested');
      expect(source).toContain('await shutdownCoordinator.waitForShutdown()');
      expect(source).not.toContain("process.once('SIGINT'");
      expect(source).not.toContain("process.once('SIGTERM'");
    }
  });

  it('keeps wall clock, randomness, identifiers, and timers behind application ports', async () => {
    const directories = [
      new URL('../../packages/alarms/src/domain/', import.meta.url),
      new URL('../../packages/alarms/src/application/', import.meta.url),
    ];
    const forbidden =
      /\b(?:Date\.now|performance\.now|Math\.random|randomUUID|setTimeout|setInterval)\s*\(/;

    for (const directory of directories) {
      const files = (await readdir(directory)).filter((file) => file.endsWith('.ts'));
      for (const file of files) {
        const source = await readFile(new URL(file, directory), 'utf8');
        expect(source, `${file} uses a runtime primitive directly`).not.toMatch(forbidden);
      }
    }
  });

  it('bounds the history grace to the PostgreSQL integer accepted by reconciliation SQL', () => {
    const key = 'PROVIDER_HISTORY_AUTHORITATIVE_EMPTY_GRACE_MS';
    const original = process.env[key];
    try {
      process.env[key] = '2147483647';
      expect(authoritativeEmptyGraceMs()).toBe(2_147_483_647);

      for (const invalid of ['-1', '1.5', '2147483648', '9007199254740991']) {
        process.env[key] = invalid;
        expect(authoritativeEmptyGraceMs).toThrow(/0 through 2147483647/);
      }
    } finally {
      if (original === undefined) delete process.env[key];
      else process.env[key] = original;
    }
  });

  it('never passes raw Error content through production log boundaries', async () => {
    const files = [
      new URL('../../apps/migrate/src/main.ts', import.meta.url),
      new URL('../../apps/worker/src/main.ts', import.meta.url),
      new URL('../../apps/worker/src/dispatch-worker.runner.ts', import.meta.url),
      new URL('../../apps/worker/src/expansion-worker.runner.ts', import.meta.url),
      new URL('../../apps/worker/src/maintenance-worker.runner.ts', import.meta.url),
      new URL('../../apps/worker/src/reconciliation-worker.runner.ts', import.meta.url),
      new URL(
        '../../packages/alarms/src/infrastructure/persistence/postgres-pool-observability.ts',
        import.meta.url,
      ),
      new URL(
        '../../packages/alarms/src/presentation/problem-exception.filter.ts',
        import.meta.url,
      ),
    ];
    const sources = await Promise.all(files.map((file) => readFile(file, 'utf8')));
    const directRawArgument =
      /(?:logger\.error|process\.stderr\.write)\(\s*(?:error|exception)\s*\)/;
    const rawOptionalArgument = /logger\.error\([\s\S]{0,200},\s*(?:error|exception)\s*\)/;

    for (const source of sources) {
      expect(source).toContain('safeErrorMetadata');
      expect(source).not.toMatch(/\berror\.(?:message|stack|cause)\b/);
      expect(source).not.toMatch(directRawArgument);
      expect(source).not.toMatch(rawOptionalArgument);
    }

    const problemFilter = sources.at(-1) ?? '';
    const catchStart = problemFilter.indexOf('public catch(');
    const conversionStart = problemFilter.indexOf('private toProblem(');
    expect(catchStart).toBeGreaterThanOrEqual(0);
    expect(conversionStart).toBeGreaterThan(catchStart);
    expect(problemFilter.slice(catchStart, conversionStart)).not.toMatch(
      /\bexception\.(?:message|stack|cause)\b/,
    );
  });
});
