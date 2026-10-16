import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { RequestMethod } from '@nestjs/common';
import { METHOD_METADATA, PATH_METADATA } from '@nestjs/common/constants';
import { describe, expect, it } from 'vitest';

import { DocsController, resolveOpenApiDocumentPath } from '../../apps/api/src/docs.controller';
import { HealthController } from '../../apps/api/src/health.controller';
import {
  ALARM_STATUSES,
  ALARM_TYPES,
  validateCreateAlarm,
} from '../../packages/alarms/src/domain/alarm';
import { AlarmsController } from '../../packages/alarms/src/presentation/alarms.controller';
import {
  collectLocalReferences,
  getOperation,
  getResponseSchema,
  HTTP_METHODS,
  loadOpenApiDocument,
  type OpenApiDocument,
  type OpenApiReference,
  type OpenApiSchema,
  resolveLocalReference,
  validateAlarmProjectionInvariants,
  validateOpenApiSchema,
} from '../support/openapi-contract';

function runtimeOperations(controller: { readonly prototype: object }): readonly string[] {
  const root = (Reflect.getMetadata(PATH_METADATA, controller) as string | undefined) ?? '';

  return Object.getOwnPropertyNames(controller.prototype).flatMap((name) => {
    if (name === 'constructor') return [];
    const handler = (controller.prototype as Record<string, unknown>)[name];
    if (typeof handler !== 'function') return [];

    const method = Reflect.getMetadata(METHOD_METADATA, handler) as RequestMethod | undefined;
    const child = (Reflect.getMetadata(PATH_METADATA, handler) as string | undefined) ?? '';
    if (method === undefined) return [];

    const path =
      `/${root}/${child}`
        .replaceAll(/\/+/g, '/')
        .replaceAll(/:([A-Za-z][A-Za-z0-9_]*)/g, '{$1}')
        .replace(/\/$/, '') || '/';
    return [`${RequestMethod[method]?.toLowerCase()} ${path}`];
  });
}

function quotedSqlValues(source: string, column: 'type' | 'status'): readonly string[] {
  const match = source.match(
    new RegExp(
      `${column}\\s+text\\s+NOT NULL\\s+CHECK\\s*\\(\\s*${column}\\s+IN\\s*\\(([^)]*)\\)`,
      's',
    ),
  );
  if (match?.[1] === undefined) throw new Error(`Could not find alarms.${column} CHECK enum`);
  return [...match[1].matchAll(/'([A-Z_]+)'/g)].map((entry) => entry[1] as string);
}

function markdownEnum(source: string, label: 'AlarmType' | 'AlarmStatus'): readonly string[] {
  const lines = source.split(/\r?\n/);
  const start = lines.findIndex((candidate) => candidate.startsWith(`- \`${label}\`:`));
  if (start < 0) throw new Error(`Could not find README ${label} contract line`);
  const contractLines = [lines[start] as string];
  for (let index = start + 1; index < lines.length && lines[index]?.startsWith('  '); index += 1) {
    contractLines.push(lines[index] as string);
  }
  return [...contractLines.join(' ').matchAll(/`([A-Z][A-Z_]*)`/g)].map(
    (entry) => entry[1] as string,
  );
}

function operationKeys(document: OpenApiDocument): readonly string[] {
  return Object.entries(document.paths).flatMap(([path, item]) =>
    HTTP_METHODS.filter((method) => item[method] !== undefined).map(
      (method) => `${method} ${path}`,
    ),
  );
}

function requestSchema(document: OpenApiDocument): OpenApiSchema {
  const schema = getOperation(document, '/v1/alarms', 'post').requestBody?.content?.[
    'application/json'
  ]?.schema;
  if (schema === undefined) throw new Error('Create request schema is missing');
  return schema;
}

const emptyAlarm = {
  id: '4ba646af-219d-4a92-aa75-2cfcb19874ef',
  type: 'BULK',
  title: 'Service notice',
  body: 'Maintenance starts at 02:00.',
  status: 'CREATED',
  failureCode: null,
  materializedCount: 0,
  total: null,
  sent: 0,
  failed: 0,
  canceled: 0,
  active: 0,
  unresolved: 0,
  duplicateCount: 0,
  incidentCount: 0,
  incidentSummary: {
    count: 0,
    codes: [],
    firstDetectedAt: null,
    lastDetectedAt: null,
  },
  audienceFinalizedAt: null,
  expansionCompletedAt: null,
  finishedAt: null,
  createdAt: '2026-10-15T00:00:00.000Z',
  updatedAt: '2026-10-15T00:00:00.000Z',
} as const;

const incidentAlarm = {
  ...emptyAlarm,
  status: 'ATTENTION_REQUIRED',
  total: 2,
  sent: 1,
  failed: 1,
  incidentCount: 2,
  incidentSummary: {
    count: 2,
    codes: [
      { code: 'LATE_DISTINCT_DUPLICATE', count: 1 },
      { code: 'LATE_POSITIVE_AFTER_FAILED', count: 1 },
    ],
    firstDetectedAt: '2026-10-15T00:00:01.000Z',
    lastDetectedAt: '2026-10-15T00:00:02.000Z',
  },
} as const;

describe('OpenAPI contract', () => {
  it('resolves the document from the API package working directory', async () => {
    const originalWorkingDirectory = process.cwd();
    try {
      process.chdir(fileURLToPath(new URL('../../apps/api/', import.meta.url)));
      const document = JSON.parse(await readFile(resolveOpenApiDocumentPath(), 'utf8')) as {
        openapi?: unknown;
      };
      expect(document.openapi).toMatch(/^3\.0\.\d+$/);
    } finally {
      process.chdir(originalWorkingDirectory);
    }
  });

  it('is structurally valid OpenAPI 3.0 JSON with no dangling or external references', async () => {
    const document = await loadOpenApiDocument();

    expect(document.openapi).toMatch(/^3\.0\.\d+$/);
    expect(document.info).toMatchObject({
      title: expect.any(String),
      version: expect.any(String),
    });

    const references = collectLocalReferences(document);
    expect(references.length).toBeGreaterThan(0);
    for (const reference of references) {
      expect(reference).toMatch(/^#\//);
      expect(() => resolveLocalReference(document, reference)).not.toThrow();
    }

    const operationIds: string[] = [];
    for (const [path, item] of Object.entries(document.paths)) {
      expect(path).toMatch(/^\//);
      for (const method of HTTP_METHODS) {
        const operation = item[method];
        if (operation === undefined) continue;

        expect(operation.operationId).toEqual(expect.any(String));
        operationIds.push(operation.operationId as string);
        expect(Object.keys(operation.responses ?? {})).not.toHaveLength(0);
        for (const [status, responseOrReference] of Object.entries(operation.responses ?? {})) {
          expect(status).toMatch(/^(?:[1-5]\d\d|default)$/);
          const response =
            responseOrReference.$ref === undefined
              ? responseOrReference
              : resolveLocalReference(document, responseOrReference.$ref);
          expect(response.description).toEqual(expect.any(String));
        }

        const pathVariables = [...path.matchAll(/\{([^}]+)\}/g)].map((match) => match[1]);
        const pathParameters = (operation.parameters ?? [])
          .map((parameter) =>
            '$ref' in parameter
              ? resolveLocalReference<Record<string, unknown>>(
                  document,
                  (parameter as OpenApiReference).$ref,
                )
              : parameter,
          )
          .filter((parameter) => parameter.in === 'path')
          .map((parameter) => parameter.name);
        expect(pathParameters.sort()).toEqual(pathVariables.sort());
      }
    }
    expect(new Set(operationIds).size).toBe(operationIds.length);
  });

  it('has exact parity with every Nest controller route and deliberately documents no 405/406', async () => {
    const document = await loadOpenApiDocument();
    const runtime = [
      ...runtimeOperations(AlarmsController),
      ...runtimeOperations(HealthController),
      ...runtimeOperations(DocsController),
    ];

    expect(operationKeys(document).sort()).toEqual(runtime.sort());
    for (const item of Object.values(document.paths)) {
      for (const method of HTTP_METHODS) {
        const statuses = Object.keys(item[method]?.responses ?? {});
        expect(statuses).not.toContain('405');
        expect(statuses).not.toContain('406');
      }
    }
  });

  it('validates every documented create example and exact request boundary semantics', async () => {
    const document = await loadOpenApiDocument();
    const schema = requestSchema(document);
    const media = getOperation(document, '/v1/alarms', 'post').requestBody?.content?.[
      'application/json'
    ];

    for (const example of Object.values(media?.examples ?? {})) {
      expect(validateOpenApiSchema(document, schema, example.value)).toEqual([]);
    }

    const exactOpaqueRecipients = ['not-a-uuid', 'A', 'Ａ', 'é', 'é'];
    const acceptedDraft = {
      type: 'EMERGENCY',
      title: `  ${'😀'.repeat(200)}  `,
      body: ' body ',
      recipientIds: exactOpaqueRecipients,
    } as const;
    expect(validateOpenApiSchema(document, schema, acceptedDraft)).toEqual([]);
    expect(validateCreateAlarm(acceptedDraft)).toEqual({
      ...acceptedDraft,
      title: '😀'.repeat(200),
    });
    expect(
      validateOpenApiSchema(document, document.components.schemas.EmergencyAlarmRequest, {
        type: 'EMERGENCY',
        title: '😀'.repeat(201),
        body: 'body',
        recipientIds: ['recipient'],
      }),
    ).toContainEqual(expect.stringContaining('normalized value must contain at most 200'));
    expect(
      validateOpenApiSchema(document, schema, {
        type: 'EMERGENCY',
        title: 'title',
        body: '   ',
        recipientIds: [' recipient'],
      }),
    ).not.toEqual([]);
    expect(
      validateOpenApiSchema(document, schema, {
        type: 'BULK',
        title: 'title',
        body: 'body',
        recipientIds: ['forbidden'],
      }),
    ).not.toEqual([]);
  });

  it('keeps UUID, Unicode length, opaque-recipient, and pagination constraints exact', async () => {
    const document = await loadOpenApiDocument();
    const bulk = document.components.schemas.BulkAlarmRequest;
    const emergency = document.components.schemas.EmergencyAlarmRequest;
    const alarm = document.components.schemas.Alarm;
    const recipientIds = emergency.properties?.recipientIds;
    const recipient = recipientIds?.items;
    const limit = getOperation(document, '/v1/alarms', 'get')
      .parameters?.map((parameter) =>
        '$ref' in parameter
          ? resolveLocalReference<Record<string, unknown>>(document, parameter.$ref)
          : parameter,
      )
      .find((parameter) => parameter.name === 'limit') as
      | { readonly schema?: OpenApiSchema }
      | undefined;
    const alarmId = resolveLocalReference<{ readonly schema?: OpenApiSchema }>(
      document,
      '#/components/parameters/AlarmId',
    );

    for (const request of [bulk, emergency]) {
      expect(request.additionalProperties).toBe(false);
      expect(request.properties?.title).toMatchObject({
        type: 'string',
        'x-normalization': 'trim',
        'x-normalized-minLength': 1,
        'x-normalized-maxLength': 200,
      });
      expect(request.properties?.body).toMatchObject({
        type: 'string',
        minLength: 1,
        maxLength: 5_000,
        'x-reject-whitespace-only': true,
      });
    }
    expect(alarm.properties?.id).toMatchObject({ type: 'string', format: 'uuid' });
    expect(alarmId.schema).toMatchObject({ type: 'string', format: 'uuid' });
    expect(recipientIds).toMatchObject({
      type: 'array',
      minItems: 1,
      maxItems: 100,
      uniqueItems: true,
    });
    expect(recipient).toMatchObject({
      type: 'string',
      minLength: 1,
      maxLength: 128,
      'x-normalization': 'none',
      'x-reject-edge-whitespace': true,
    });
    expect(recipient?.format).toBeUndefined();
    expect(limit?.schema).toMatchObject({
      type: 'integer',
      minimum: 1,
      maximum: 100,
      default: 50,
    });
  });

  it('validates representative success, list, problem, and health documents including invariants', async () => {
    const document = await loadOpenApiDocument();
    const examples: readonly [string, OpenApiSchema, unknown][] = [
      [
        'created Alarm',
        getResponseSchema(document, '/v1/alarms', 'post', 201, 'application/json'),
        emptyAlarm,
      ],
      [
        'incident Alarm',
        getResponseSchema(document, '/v1/alarms/{alarmId}', 'get', 200, 'application/json'),
        incidentAlarm,
      ],
      [
        'Alarm page',
        getResponseSchema(document, '/v1/alarms', 'get', 200, 'application/json'),
        { alarms: [emptyAlarm, incidentAlarm], nextCursor: null },
      ],
      [
        'Problem',
        getResponseSchema(document, '/v1/alarms', 'post', 422, 'application/problem+json'),
        {
          type: 'about:blank',
          title: 'Unprocessable Entity',
          status: 422,
          code: 'INVALID_ALARM',
          detail: 'Alarm input is invalid',
          instance: '/v1/alarms',
          traceId: 'request-1',
          errors: [{ field: 'title', code: 'INVALID_LENGTH' }],
        },
      ],
      [
        'Health',
        getResponseSchema(document, '/health/live', 'get', 200, 'application/json'),
        { ok: true },
      ],
    ];

    for (const [label, schema, value] of examples) {
      expect(validateOpenApiSchema(document, schema, value), label).toEqual([]);
    }
    expect(validateAlarmProjectionInvariants(emptyAlarm)).toEqual([]);
    expect(validateAlarmProjectionInvariants(incidentAlarm)).toEqual([]);
  });

  it('keeps code, alarm-table checks, OpenAPI, and README type/status enums identical', async () => {
    const [document, migration, readme] = await Promise.all([
      loadOpenApiDocument(),
      readFile(new URL('../../migrations/0001_alarm_delivery_core.sql', import.meta.url), 'utf8'),
      readFile(new URL('../../README.md', import.meta.url), 'utf8'),
    ]);
    const openApiTypes = document.components.schemas.AlarmType?.enum;
    const openApiStatuses = document.components.schemas.AlarmStatus?.enum;

    expect(openApiTypes).toEqual(ALARM_TYPES);
    expect(quotedSqlValues(migration, 'type')).toEqual(ALARM_TYPES);
    expect(markdownEnum(readme, 'AlarmType')).toEqual(ALARM_TYPES);
    expect(openApiStatuses).toEqual(ALARM_STATUSES);
    expect(quotedSqlValues(migration, 'status')).toEqual(ALARM_STATUSES);
    expect(markdownEnum(readme, 'AlarmStatus')).toEqual(ALARM_STATUSES);
  });

  it('documents exact response surfaces including internal failures and parser failures', async () => {
    const document = await loadOpenApiDocument();
    const expected = new Map<string, readonly string[]>([
      ['post /v1/alarms', ['201', '400', '413', '415', '422', '500', '503']],
      ['get /v1/alarms', ['200', '400', '422', '500', '503']],
      ['get /v1/alarms/{alarmId}', ['200', '400', '404', '500', '503']],
      ['post /v1/alarms/{alarmId}/send', ['202', '400', '404', '409', '500', '503']],
      ['post /v1/alarms/{alarmId}/cancel', ['202', '400', '404', '409', '500', '503']],
      ['get /health/live', ['200', '500']],
      ['get /health/ready', ['200', '500', '503']],
      ['get /docs', ['200', '500']],
      ['get /docs/openapi.json', ['200', '500']],
    ]);

    for (const [operation, statuses] of expected) {
      const [method, path] = operation.split(' ') as [(typeof HTTP_METHODS)[number], string];
      expect(Object.keys(getOperation(document, path, method).responses ?? {}).sort()).toEqual(
        [...statuses].sort(),
      );
    }
  });
});
