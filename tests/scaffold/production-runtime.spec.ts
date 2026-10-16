import { readFile } from 'node:fs/promises';

import { describe, expect, it } from 'vitest';

interface DockerInstruction {
  readonly operation: string;
  readonly value: string;
}

interface DockerStage {
  readonly image: string;
  readonly name: string | null;
  readonly instructions: readonly DockerInstruction[];
}

function logicalDockerfileLines(source: string): string[] {
  const logical: string[] = [];
  let pending = '';
  for (const rawLine of source.split(/\r?\n/)) {
    const trimmed = rawLine.trim();
    if (trimmed.length === 0 || trimmed.startsWith('#')) continue;
    pending = `${pending}${pending.length === 0 ? '' : ' '}${trimmed}`;
    if (pending.endsWith('\\')) {
      pending = pending.slice(0, -1).trimEnd();
      continue;
    }
    logical.push(pending);
    pending = '';
  }
  if (pending.length > 0) logical.push(pending);
  return logical;
}

function parseDockerfile(source: string): DockerStage[] {
  const stages: Array<{
    image: string;
    name: string | null;
    instructions: DockerInstruction[];
  }> = [];
  for (const line of logicalDockerfileLines(source)) {
    const separator = line.indexOf(' ');
    const operation = (separator < 0 ? line : line.slice(0, separator)).toUpperCase();
    const value = separator < 0 ? '' : line.slice(separator + 1).trim();
    if (operation === 'FROM') {
      const match = /^(\S+)(?:\s+AS\s+(\S+))?$/i.exec(value);
      if (match?.[1] === undefined) throw new Error(`Invalid FROM instruction: ${line}`);
      stages.push({ image: match[1], name: match[2] ?? null, instructions: [] });
      continue;
    }
    const stage = stages.at(-1);
    if (stage === undefined) throw new Error(`Instruction before FROM: ${line}`);
    stage.instructions.push({ operation, value });
  }
  return stages;
}

function composeService(source: string, serviceName: string): string[] {
  const lines = source.split(/\r?\n/);
  const start = lines.indexOf(`  ${serviceName}:`);
  if (start < 0) throw new Error(`Missing Compose service ${serviceName}`);
  let end = lines.length;
  for (let index = start + 1; index < lines.length; index += 1) {
    const line = lines[index] ?? '';
    if (/^ {2}[A-Za-z0-9_-]+:\s*$/.test(line) || (/^\S/.test(line) && line.trim().length > 0)) {
      end = index;
      break;
    }
  }
  return lines.slice(start + 1, end);
}

function scalarValue(lines: readonly string[], key: string): string | null {
  const prefix = `    ${key}:`;
  const line = lines.find((candidate) => candidate.startsWith(prefix));
  return line === undefined ? null : line.slice(prefix.length).trim();
}

describe('production container runtime contract', () => {
  it('OPS-13 runs only built artifacts as a non-root user in the final image', async () => {
    const dockerfile = await readFile(new URL('../../Dockerfile', import.meta.url), 'utf8');
    const stages = parseDockerfile(dockerfile);
    const runtime = stages.find(({ name }) => name === 'runtime');
    if (runtime === undefined) throw new Error('Missing runtime Docker stage');

    const users = runtime.instructions.filter(({ operation }) => operation === 'USER');
    const commands = runtime.instructions.filter(({ operation }) => operation === 'CMD');
    const copies = runtime.instructions
      .filter(({ operation }) => operation === 'COPY')
      .map(({ value }) => value);

    expect(runtime.image).toBe('node:24.19.0-alpine');
    expect(users.map(({ value }) => value)).toEqual(['node']);
    expect(commands).toHaveLength(1);
    expect(JSON.parse(commands[0]?.value ?? 'null')).toEqual(['node', 'apps/api/dist/src/main.js']);
    expect(
      copies.some((copy) =>
        /(?:^|\s)(?:\.\/)?(?:apps|packages)\/[^\s]+\/src(?:\/|\s|$)/.test(copy),
      ),
    ).toBe(false);
    expect(copies.some((copy) => /(?:^|\s)\.\s+\.(?:\s|$)/.test(copy))).toBe(false);
    expect(copies.filter((copy) => copy.includes('/dist'))).toHaveLength(5);
    expect(
      runtime.instructions.some(({ value }) => /\b(?:ts-node|watch|hot-reload)\b/i.test(value)),
    ).toBe(false);
  });

  it('OPS-13 uses production node commands and no source bind mount for API/worker', async () => {
    const compose = await readFile(new URL('../../docker-compose.yml', import.meta.url), 'utf8');
    const api = composeService(compose, 'api');
    const worker = composeService(compose, 'worker');

    expect(scalarValue(api, 'build')).toBe('');
    expect(api.some((line) => line.trim() === 'context: .')).toBe(true);
    expect(scalarValue(worker, 'build')).toBe('');
    expect(worker.some((line) => line.trim() === 'context: .')).toBe(true);
    expect(api.some((line) => line.trim() === 'volumes:')).toBe(false);
    expect(worker.some((line) => line.trim() === 'volumes:')).toBe(false);
    expect(scalarValue(api, 'command')).toBeNull();
    expect(JSON.parse(scalarValue(worker, 'command') ?? 'null')).toEqual([
      'node',
      'apps/worker/dist/src/main.js',
    ]);
    expect([...api, ...worker].some((line) => /\b(?:ts-node|watch|hot-reload)\b/i.test(line))).toBe(
      false,
    );
  });
});
