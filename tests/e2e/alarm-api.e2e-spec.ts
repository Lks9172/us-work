import 'reflect-metadata';

import { resolve } from 'node:path';

import { Logger } from '@nestjs/common';
import { FastifyAdapter, type NestFastifyApplication } from '@nestjs/platform-fastify';
import { Test } from '@nestjs/testing';
import { Pool } from 'pg';
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';

import { ApiModule } from '../../apps/api/src/api.module';
import { API_BODY_LIMIT_BYTES, API_FASTIFY_OPTIONS } from '../../apps/api/src/http-server-config';
import { AlarmApplicationService } from '../../packages/alarms/src/application/alarm-service';
import { runMigrations } from '../../packages/alarms/src/infrastructure/persistence/migrations';
import { ALARM_APPLICATION_SERVICE } from '../../packages/alarms/src/presentation/alarm-presentation.tokens';
import { createTestDatabase, type TestDatabase } from '../integration/postgres-test-schema';
import {
  getOperation,
  getResponse,
  getResponseSchema,
  loadOpenApiDocument,
  type OpenApiDocument,
  type OpenApiHttpMethod,
  resolveLocalReference,
  validateAlarmProjectionInvariants,
  validateOpenApiSchema,
} from '../support/openapi-contract';

const databaseUrl = process.env.TEST_DATABASE_URL;
const describeWithDatabase = databaseUrl === undefined ? describe.skip : describe;

interface AlarmResponse {
  readonly id: string;
  readonly type: 'BULK' | 'EMERGENCY';
  readonly title: string;
  readonly body: string;
  readonly status: string;
  readonly total: number | null;
}

interface ProblemResponse {
  readonly type: string;
  readonly title: string;
  readonly status: number;
  readonly code: string;
  readonly detail: string;
  readonly instance: string;
  readonly traceId: string;
}

interface InjectedResponse {
  readonly statusCode: number;
  readonly headers: Readonly<Record<string, string | string[] | undefined>>;
  json<T = unknown>(): T;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function expectDocumentedResponse(
  document: OpenApiDocument,
  response: InjectedResponse,
  path: string,
  method: OpenApiHttpMethod,
  status: number,
  mediaType: 'application/json' | 'application/problem+json',
): unknown {
  expect(response.statusCode).toBe(status);
  expect(response.headers['content-type']).toContain(mediaType);
  const body = response.json<unknown>();
  const schema = getResponseSchema(document, path, method, status, mediaType);
  expect(validateOpenApiSchema(document, schema, body)).toEqual([]);

  for (const [name, referenceOrHeader] of Object.entries(
    getResponse(document, path, method, status).headers ?? {},
  )) {
    const header =
      '$ref' in referenceOrHeader
        ? resolveLocalReference<{ readonly schema?: Parameters<typeof validateOpenApiSchema>[1] }>(
            document,
            referenceOrHeader.$ref,
          )
        : referenceOrHeader;
    const value = response.headers[name.toLowerCase()];
    expect(value, `${status} ${name} response header`).toEqual(expect.any(String));
    if (header.schema !== undefined) {
      expect(validateOpenApiSchema(document, header.schema, value)).toEqual([]);
    }
  }

  if (isRecord(body) && Object.hasOwn(body, 'incidentSummary')) {
    expect(validateAlarmProjectionInvariants(body)).toEqual([]);
  }
  if (isRecord(body) && Array.isArray(body.alarms)) {
    for (const alarm of body.alarms) {
      expect(validateAlarmProjectionInvariants(alarm)).toEqual([]);
    }
  }

  return body;
}

function expectUndocumentedRouteProblem(
  document: OpenApiDocument,
  response: InjectedResponse,
): void {
  expect(response.statusCode).toBe(404);
  expect(response.headers['content-type']).toContain('application/problem+json');
  expect(
    validateOpenApiSchema(document, { $ref: '#/components/schemas/Problem' }, response.json()),
  ).toEqual([]);
  expect(response.json<ProblemResponse>()).toMatchObject({
    type: 'about:blank',
    title: 'Not Found',
    status: 404,
    code: 'HTTP_ERROR',
    traceId: expect.any(String),
  });
}

describeWithDatabase('Alarm HTTP API', () => {
  let application: NestFastifyApplication;
  let testDatabase: TestDatabase;
  let pool: Pool;
  let openApiDocument: OpenApiDocument;

  beforeAll(async () => {
    if (databaseUrl === undefined) {
      throw new Error('TEST_DATABASE_URL is required');
    }

    testDatabase = await createTestDatabase(databaseUrl);
    await runMigrations({
      databaseUrl: testDatabase.databaseUrl,
      migrationsDirectory: resolve(process.cwd(), 'migrations'),
    });

    process.env.DATABASE_URL = testDatabase.databaseUrl;
    process.env.CURSOR_SIGNING_KEY = 'e2e-only-cursor-signing-key';
    pool = new Pool({ connectionString: testDatabase.databaseUrl });
    openApiDocument = await loadOpenApiDocument();

    const module = await Test.createTestingModule({ imports: [ApiModule] }).compile();
    application = module.createNestApplication<NestFastifyApplication>(
      new FastifyAdapter(API_FASTIFY_OPTIONS),
    );
    await application.init();
    await application.getHttpAdapter().getInstance().ready();
  });

  afterAll(async () => {
    await application?.close();
    await pool?.end();
    await testDatabase?.cleanup();
    delete process.env.DATABASE_URL;
    delete process.env.CURSOR_SIGNING_KEY;
  });

  it('DOC-07 executes every documented create example and validates the actual response', async () => {
    const media = getOperation(openApiDocument, '/v1/alarms', 'post').requestBody?.content?.[
      'application/json'
    ];
    const examples = Object.entries(media?.examples ?? {});
    expect(examples.length).toBeGreaterThan(0);

    for (const [name, example] of examples) {
      const payload = example.value as {
        readonly type: 'BULK' | 'EMERGENCY';
        readonly title: string;
        readonly body: string;
      };
      const response = await application.inject({
        method: 'POST',
        url: '/v1/alarms',
        payload,
      });
      const body = expectDocumentedResponse(
        openApiDocument,
        response,
        '/v1/alarms',
        'post',
        201,
        'application/json',
      ) as AlarmResponse;

      expect(body, name).toMatchObject({
        type: payload.type,
        title: payload.title.trim(),
        body: payload.body,
        status: 'CREATED',
      });
      expect(response.headers.location).toBe(`/v1/alarms/${body.id}`);
    }
  });

  it('DOC-08 serves the interactive Swagger UI and its canonical JSON in the real API app', async () => {
    const ui = await application.inject({ method: 'GET', url: '/docs' });
    expect(ui.statusCode).toBe(200);
    expect(ui.headers['content-type']).toContain('text/html');
    expect(ui.body).toContain('SwaggerUIBundle');
    expect(ui.body).toContain("url: '/docs/openapi.json'");

    const json = await application.inject({ method: 'GET', url: '/docs/openapi.json' });
    expect(json.statusCode).toBe(200);
    expect(json.headers['content-type']).toContain('application/json');
    expect(json.json()).toEqual(openApiDocument);
  });

  it('creates, starts, queries, and idempotently restarts an emergency alarm', async () => {
    const createResponse = await application.inject({
      method: 'POST',
      url: '/v1/alarms',
      payload: {
        type: 'EMERGENCY',
        title: 'Payment authorization',
        body: 'Approve the payment now.',
        recipientIds: ['user-001', 'user-002'],
      },
    });
    const created = createResponse.json<AlarmResponse>();

    expect(createResponse.statusCode).toBe(201);
    expectDocumentedResponse(
      openApiDocument,
      createResponse,
      '/v1/alarms',
      'post',
      201,
      'application/json',
    );
    expect(createResponse.headers.location).toBe(`/v1/alarms/${created.id}`);
    expect(created).toMatchObject({
      type: 'EMERGENCY',
      status: 'CREATED',
      total: null,
    });

    const startResponse = await application.inject({
      method: 'POST',
      url: `/v1/alarms/${created.id}/send`,
    });
    const started = startResponse.json<AlarmResponse>();
    const duplicateStartResponse = await application.inject({
      method: 'POST',
      url: `/v1/alarms/${created.id}/send`,
    });

    expect(startResponse.statusCode).toBe(202);
    expectDocumentedResponse(
      openApiDocument,
      startResponse,
      '/v1/alarms/{alarmId}/send',
      'post',
      202,
      'application/json',
    );
    expect(started).toMatchObject({ status: 'SENDING', total: 2 });
    expect(duplicateStartResponse.statusCode).toBe(202);
    expectDocumentedResponse(
      openApiDocument,
      duplicateStartResponse,
      '/v1/alarms/{alarmId}/send',
      'post',
      202,
      'application/json',
    );
    expect(duplicateStartResponse.json<AlarmResponse>()).toMatchObject({
      status: 'SENDING',
      total: 2,
    });

    const detailResponse = await application.inject({
      method: 'GET',
      url: `/v1/alarms/${created.id}`,
    });
    expect(detailResponse.statusCode).toBe(200);
    expectDocumentedResponse(
      openApiDocument,
      detailResponse,
      '/v1/alarms/{alarmId}',
      'get',
      200,
      'application/json',
    );
    expect(detailResponse.json<AlarmResponse>()).toMatchObject({
      id: created.id,
      status: 'SENDING',
    });
  });

  it('accepts the maximum domain-valid Unicode emergency payload within the byte limit', async () => {
    const title = '😀'.repeat(200);
    const body = '🚨'.repeat(5_000);
    const recipientIds = Array.from(
      { length: 100 },
      (_, index) => `u${index.toString().padStart(3, '0')}${'🙂'.repeat(124)}`,
    );
    const response = await application.inject({
      method: 'POST',
      url: '/v1/alarms',
      payload: { type: 'EMERGENCY', title, body, recipientIds },
    });

    expect(
      Buffer.byteLength(JSON.stringify({ type: 'EMERGENCY', title, body, recipientIds })),
    ).toBeGreaterThan(65_536);
    expect(response.statusCode).toBe(201);
    expect(response.json<AlarmResponse>()).toMatchObject({ title, body, status: 'CREATED' });
  });

  it('uses stable cursor pagination and cancels an unstarted bulk alarm', async () => {
    const firstCreate = await application.inject({
      method: 'POST',
      url: '/v1/alarms',
      payload: { type: 'BULK', title: 'First notice', body: 'First body' },
    });
    const first = firstCreate.json<AlarmResponse>();

    const secondCreate = await application.inject({
      method: 'POST',
      url: '/v1/alarms',
      payload: { type: 'BULK', title: 'Second notice', body: 'Second body' },
    });
    const second = secondCreate.json<AlarmResponse>();

    const firstPageResponse = await application.inject({
      method: 'GET',
      url: '/v1/alarms?type=BULK&limit=1',
    });
    const firstPage = firstPageResponse.json<{
      alarms: AlarmResponse[];
      nextCursor: string | null;
    }>();

    expect(firstPageResponse.statusCode).toBe(200);
    expectDocumentedResponse(
      openApiDocument,
      firstPageResponse,
      '/v1/alarms',
      'get',
      200,
      'application/json',
    );
    expect(firstPage.alarms).toHaveLength(1);
    expect(firstPage.nextCursor).toEqual(expect.any(String));

    const secondPageResponse = await application.inject({
      method: 'GET',
      url: `/v1/alarms?type=BULK&limit=1&cursor=${encodeURIComponent(firstPage.nextCursor ?? '')}`,
    });
    const secondPage = secondPageResponse.json<{ alarms: AlarmResponse[] }>();
    expect(secondPageResponse.statusCode).toBe(200);
    expect(secondPage.alarms).toHaveLength(1);
    expect(secondPage.alarms[0]?.id).not.toBe(firstPage.alarms[0]?.id);

    const cancelResponse = await application.inject({
      method: 'POST',
      url: `/v1/alarms/${second.id}/cancel`,
    });
    expect(cancelResponse.statusCode).toBe(202);
    expectDocumentedResponse(
      openApiDocument,
      cancelResponse,
      '/v1/alarms/{alarmId}/cancel',
      'post',
      202,
      'application/json',
    );
    expect(cancelResponse.json<AlarmResponse>()).toMatchObject({
      id: second.id,
      status: 'CANCELED',
      total: 0,
    });

    expect([first.id, second.id]).toContain(firstPage.alarms[0]?.id);
  });

  it('returns deterministic problem details for malformed and invalid requests', async () => {
    const invalidAlarm = await application.inject({
      method: 'POST',
      url: '/v1/alarms',
      payload: {
        type: 'BULK',
        title: 'Notice',
        body: 'Body',
        recipientIds: ['not-allowed-for-bulk'],
      },
    });
    const invalidId = await application.inject({
      method: 'GET',
      url: '/v1/alarms/not-a-uuid',
    });
    const missing = await application.inject({
      method: 'GET',
      url: '/v1/alarms/00000000-0000-4000-8000-000000000001',
    });

    expect(invalidAlarm.statusCode).toBe(422);
    expectDocumentedResponse(
      openApiDocument,
      invalidAlarm,
      '/v1/alarms',
      'post',
      422,
      'application/problem+json',
    );
    expect(invalidAlarm.headers['content-type']).toContain('application/problem+json');
    expect(invalidAlarm.json()).toMatchObject({
      status: 422,
      code: 'INVALID_ALARM',
    });
    expect(invalidId.statusCode).toBe(400);
    expectDocumentedResponse(
      openApiDocument,
      invalidId,
      '/v1/alarms/{alarmId}',
      'get',
      400,
      'application/problem+json',
    );
    expect(invalidId.json()).toMatchObject({ status: 400, code: 'INVALID_REQUEST' });
    expect(missing.statusCode).toBe(404);
    expectDocumentedResponse(
      openApiDocument,
      missing,
      '/v1/alarms/{alarmId}',
      'get',
      404,
      'application/problem+json',
    );
    expect(missing.json()).toMatchObject({ status: 404, code: 'ALARM_NOT_FOUND' });
  });

  it('rejects every missing, null, empty, and blank required string without persistence', async () => {
    const before = await pool.query<{
      alarms: number;
      deliveries: number;
      emergency_recipients: number;
      expansion_jobs: number;
    }>(`
      SELECT
        (SELECT count(*)::integer FROM alarms) AS alarms,
        (SELECT count(*)::integer FROM deliveries) AS deliveries,
        (SELECT count(*)::integer FROM emergency_recipients) AS emergency_recipients,
        (SELECT count(*)::integer FROM alarm_expansion_jobs) AS expansion_jobs
    `);
    const invalidBodies: readonly Record<string, unknown>[] = [
      { title: 'Notice', body: 'Body' },
      { type: null, title: 'Notice', body: 'Body' },
      { type: '', title: 'Notice', body: 'Body' },
      { type: '   ', title: 'Notice', body: 'Body' },
      { type: 'BULK', body: 'Body' },
      { type: 'BULK', title: null, body: 'Body' },
      { type: 'BULK', title: '', body: 'Body' },
      { type: 'BULK', title: '   ', body: 'Body' },
      { type: 'BULK', title: 'Notice' },
      { type: 'BULK', title: 'Notice', body: null },
      { type: 'BULK', title: 'Notice', body: '' },
      { type: 'BULK', title: 'Notice', body: '   ' },
    ];

    for (const payload of invalidBodies) {
      const response = await application.inject({
        method: 'POST',
        url: '/v1/alarms',
        payload,
      });

      expect(response.statusCode).toBe(422);
      expect(response.headers['content-type']).toContain('application/problem+json');
      expect(response.json<ProblemResponse>()).toMatchObject({
        type: 'about:blank',
        status: 422,
        code: 'INVALID_ALARM',
        instance: '/v1/alarms',
        traceId: expect.any(String),
      });
    }

    await expect(
      pool.query(`
        SELECT
          (SELECT count(*)::integer FROM alarms) AS alarms,
          (SELECT count(*)::integer FROM deliveries) AS deliveries,
          (SELECT count(*)::integer FROM emergency_recipients) AS emergency_recipients,
          (SELECT count(*)::integer FROM alarm_expansion_jobs) AS expansion_jobs
      `),
    ).resolves.toMatchObject({ rows: before.rows });
  });

  it('rejects malformed JSON, non-object bodies, and unsupported media types', async () => {
    const malformedJson = await application.inject({
      method: 'POST',
      url: '/v1/alarms',
      headers: { 'content-type': 'application/json' },
      payload: '{"type":',
    });
    const arrayBody = await application.inject({
      method: 'POST',
      url: '/v1/alarms',
      headers: { 'content-type': 'application/json' },
      payload: '[]',
    });
    const nonJson = await application.inject({
      method: 'POST',
      url: '/v1/alarms',
      headers: { 'content-type': 'text/plain' },
      payload: 'not json',
    });
    const missingContentType = await application.inject({
      method: 'POST',
      url: '/v1/alarms',
    });
    const oversizedBody = await application.inject({
      method: 'POST',
      url: '/v1/alarms',
      headers: { 'content-type': 'application/json' },
      payload: JSON.stringify({
        type: 'BULK',
        title: 'Oversized',
        body: 'x'.repeat(API_BODY_LIMIT_BYTES),
      }),
    });

    expect(malformedJson.statusCode).toBe(400);
    expectDocumentedResponse(
      openApiDocument,
      malformedJson,
      '/v1/alarms',
      'post',
      400,
      'application/problem+json',
    );
    expect(malformedJson.headers['content-type']).toContain('application/problem+json');
    expect(malformedJson.json()).toMatchObject({ status: 400, code: 'INVALID_REQUEST' });
    expect(arrayBody.statusCode).toBe(400);
    expect(arrayBody.json()).toMatchObject({ status: 400, code: 'INVALID_REQUEST' });
    expect(nonJson.statusCode).toBe(415);
    expectDocumentedResponse(
      openApiDocument,
      nonJson,
      '/v1/alarms',
      'post',
      415,
      'application/problem+json',
    );
    expect(nonJson.json()).toMatchObject({ status: 415, code: 'UNSUPPORTED_MEDIA_TYPE' });
    expect(missingContentType.statusCode).toBe(415);
    expect(missingContentType.json()).toMatchObject({
      status: 415,
      code: 'UNSUPPORTED_MEDIA_TYPE',
    });
    expect(oversizedBody.statusCode).toBe(413);
    expectDocumentedResponse(
      openApiDocument,
      oversizedBody,
      '/v1/alarms',
      'post',
      413,
      'application/problem+json',
    );
    expect(oversizedBody.json()).toMatchObject({ status: 413, code: 'PAYLOAD_TOO_LARGE' });
  });

  it('returns 503 and rolls back create and start when persistence fails mid-command', async () => {
    await pool.query(`
      CREATE FUNCTION reject_emergency_snapshot() RETURNS trigger
      LANGUAGE plpgsql AS $$
      BEGIN
        RAISE EXCEPTION 'injected emergency snapshot failure';
      END
      $$
    `);
    await pool.query(`
      CREATE TRIGGER reject_emergency_snapshot_trigger
      BEFORE INSERT ON emergency_recipients
      FOR EACH ROW EXECUTE FUNCTION reject_emergency_snapshot()
    `);
    const failedCreate = await application.inject({
      method: 'POST',
      url: '/v1/alarms',
      payload: {
        type: 'EMERGENCY',
        title: 'must-roll-back-create',
        body: 'body',
        recipientIds: ['recipient'],
      },
    });
    expect(failedCreate.statusCode).toBe(503);
    expectDocumentedResponse(
      openApiDocument,
      failedCreate,
      '/v1/alarms',
      'post',
      503,
      'application/problem+json',
    );
    expect(failedCreate.json()).toMatchObject({
      status: 503,
      code: 'PERSISTENCE_UNAVAILABLE',
      traceId: expect.any(String),
    });
    await expect(
      pool.query<{ count: number }>(
        "SELECT count(*)::integer AS count FROM alarms WHERE title = 'must-roll-back-create'",
      ),
    ).resolves.toMatchObject({ rows: [{ count: 0 }] });
    await pool.query('DROP TRIGGER reject_emergency_snapshot_trigger ON emergency_recipients');
    await pool.query('DROP FUNCTION reject_emergency_snapshot()');

    const created = await application.inject({
      method: 'POST',
      url: '/v1/alarms',
      payload: { type: 'BULK', title: 'must-roll-back-start', body: 'body' },
    });
    const alarm = created.json<AlarmResponse>();
    await pool.query(`
      CREATE FUNCTION reject_expansion_job() RETURNS trigger
      LANGUAGE plpgsql AS $$
      BEGIN
        RAISE EXCEPTION 'injected expansion job failure';
      END
      $$
    `);
    await pool.query(`
      CREATE TRIGGER reject_expansion_job_trigger
      BEFORE INSERT ON alarm_expansion_jobs
      FOR EACH ROW EXECUTE FUNCTION reject_expansion_job()
    `);
    const failedStart = await application.inject({
      method: 'POST',
      url: `/v1/alarms/${alarm.id}/send`,
    });
    expect(failedStart.statusCode).toBe(503);
    expect(failedStart.json()).toMatchObject({
      status: 503,
      code: 'PERSISTENCE_UNAVAILABLE',
      traceId: expect.any(String),
    });
    const rolledBackStart = await pool.query<{ status: string; jobs: number }>(
      `
        SELECT
          a.status,
          (SELECT count(*)::integer FROM alarm_expansion_jobs j WHERE j.alarm_id = a.id) AS jobs
        FROM alarms a
        WHERE a.id = $1
      `,
      [alarm.id],
    );
    expect(rolledBackStart.rows[0]).toEqual({ status: 'CREATED', jobs: 0 });
    await pool.query('DROP TRIGGER reject_expansion_job_trigger ON alarm_expansion_jobs');
    await pool.query('DROP FUNCTION reject_expansion_job()');
  });

  it('returns isolated 503 problem details when list and detail reads fail', async () => {
    const created = await application.inject({
      method: 'POST',
      url: '/v1/alarms',
      payload: { type: 'BULK', title: 'read-failure-fixture', body: 'body' },
    });
    const alarm = created.json<AlarmResponse>();
    const before = await pool.query<{
      status: string;
      version: string;
      updated_at: string;
      delivery_count: number;
      expansion_count: number;
    }>(
      `
        SELECT
          a.status,
          a.version::text,
          a.updated_at::text,
          (SELECT count(*)::integer FROM deliveries d WHERE d.alarm_id = a.id) AS delivery_count,
          (
            SELECT count(*)::integer
            FROM alarm_expansion_jobs j
            WHERE j.alarm_id = a.id
          ) AS expansion_count
        FROM alarms a
        WHERE a.id = $1
      `,
      [alarm.id],
    );

    await pool.query('ALTER TABLE alarms RENAME TO alarms_read_failure_fixture');
    try {
      const listFailure = await application.inject({ method: 'GET', url: '/v1/alarms' });
      const detailFailure = await application.inject({
        method: 'GET',
        url: `/v1/alarms/${alarm.id}`,
      });

      for (const response of [listFailure, detailFailure]) {
        expect(response.statusCode).toBe(503);
        expect(response.headers['content-type']).toContain('application/problem+json');
        expect(response.json<ProblemResponse>()).toMatchObject({
          type: 'about:blank',
          title: 'Service Unavailable',
          status: 503,
          code: 'PERSISTENCE_UNAVAILABLE',
          detail: 'Persistence is unavailable',
          traceId: expect.any(String),
        });
      }
      expect(listFailure.json<ProblemResponse>().instance).toBe('/v1/alarms');
      expect(detailFailure.json<ProblemResponse>().instance).toBe(`/v1/alarms/${alarm.id}`);
    } finally {
      await pool.query('ALTER TABLE alarms_read_failure_fixture RENAME TO alarms');
    }

    await expect(
      pool.query(
        `
          SELECT
            a.status,
            a.version::text,
            a.updated_at::text,
            (SELECT count(*)::integer FROM deliveries d WHERE d.alarm_id = a.id) AS delivery_count,
            (
              SELECT count(*)::integer
              FROM alarm_expansion_jobs j
              WHERE j.alarm_id = a.id
            ) AS expansion_count
          FROM alarms a
          WHERE a.id = $1
        `,
        [alarm.id],
      ),
    ).resolves.toMatchObject({ rows: before.rows });
  });

  it('executes the deliberate 404/not-405 route matrix on the real Nest/Fastify app', async () => {
    const alarmId = '00000000-0000-4000-8000-000000000001';
    const cases = [
      { method: 'GET', url: '/v1/not-defined' },
      { method: 'POST', url: '/v1/not-defined' },
      { method: 'DELETE', url: '/v1/not-defined' },
      { method: 'PATCH', url: '/v1/alarms' },
      { method: 'DELETE', url: `/v1/alarms/${alarmId}` },
      { method: 'GET', url: `/v1/alarms/${alarmId}/send` },
      { method: 'PUT', url: '/health/live' },
      { method: 'HEAD', url: '/v1/alarms' },
      { method: 'HEAD', url: '/health/live' },
    ] as const;

    const statuses: number[] = [];
    for (const testCase of cases) {
      const response = await application.inject({
        ...testCase,
        headers: { accept: 'application/xml' },
      });
      statuses.push(response.statusCode);
      expectUndocumentedRouteProblem(openApiDocument, response);
      expect(response.json<ProblemResponse>().instance).toBe(testCase.url);
    }

    expect(statuses).not.toContain(405);
  });

  it('executes the deliberate JSON/not-406 Accept matrix on every public success route', async () => {
    const create = await application.inject({
      method: 'POST',
      url: '/v1/alarms',
      headers: {
        accept: 'application/xml',
        'content-type': 'application/json; charset=utf-8',
      },
      payload: {
        type: 'EMERGENCY',
        title: 'accept-matrix-send',
        body: 'body',
        recipientIds: ['opaque-recipient'],
      },
    });
    const sendAlarm = create.json<AlarmResponse>();
    expectDocumentedResponse(
      openApiDocument,
      create,
      '/v1/alarms',
      'post',
      201,
      'application/json',
    );

    const list = await application.inject({
      method: 'GET',
      url: '/v1/alarms?limit=1',
      headers: { accept: 'application/xml' },
    });
    expectDocumentedResponse(openApiDocument, list, '/v1/alarms', 'get', 200, 'application/json');

    const detail = await application.inject({
      method: 'GET',
      url: `/v1/alarms/${sendAlarm.id}`,
      headers: { accept: 'text/plain;q=1, application/json;q=0' },
    });
    expectDocumentedResponse(
      openApiDocument,
      detail,
      '/v1/alarms/{alarmId}',
      'get',
      200,
      'application/json',
    );

    const send = await application.inject({
      method: 'POST',
      url: `/v1/alarms/${sendAlarm.id}/send`,
      headers: { accept: 'application/xml' },
    });
    expectDocumentedResponse(
      openApiDocument,
      send,
      '/v1/alarms/{alarmId}/send',
      'post',
      202,
      'application/json',
    );

    const cancelFixture = await application.inject({
      method: 'POST',
      url: '/v1/alarms',
      headers: { 'content-type': 'application/json' },
      payload: { type: 'BULK', title: 'accept-matrix-cancel', body: 'body' },
    });
    const cancelAlarm = cancelFixture.json<AlarmResponse>();
    const cancel = await application.inject({
      method: 'POST',
      url: `/v1/alarms/${cancelAlarm.id}/cancel`,
      headers: { accept: 'application/xml' },
    });
    expectDocumentedResponse(
      openApiDocument,
      cancel,
      '/v1/alarms/{alarmId}/cancel',
      'post',
      202,
      'application/json',
    );

    for (const [path, response] of [
      [
        '/health/live',
        await application.inject({
          method: 'GET',
          url: '/health/live',
          headers: { accept: 'application/xml' },
        }),
      ],
      [
        '/health/ready',
        await application.inject({
          method: 'GET',
          url: '/health/ready',
          headers: { accept: 'application/xml' },
        }),
      ],
    ] as const) {
      expectDocumentedResponse(openApiDocument, response, path, 'get', 200, 'application/json');
    }

    expect([create, list, detail, send, cancel].map(({ statusCode }) => statusCode)).not.toContain(
      406,
    );
  });

  it('executes the 415 request media matrix without persisting any rejected alarm', async () => {
    const before = await pool.query<{ count: number }>(
      'SELECT count(*)::integer AS count FROM alarms',
    );
    const mediaTypes = [
      'text/plain',
      'application/xml',
      'application/problem+json',
      'application/merge-patch+json',
      'application/octet-stream',
    ] as const;

    for (const mediaType of mediaTypes) {
      const response = await application.inject({
        method: 'POST',
        url: '/v1/alarms',
        headers: { 'content-type': mediaType },
        payload: JSON.stringify({ type: 'BULK', title: 'rejected-media', body: 'body' }),
      });
      expectDocumentedResponse(
        openApiDocument,
        response,
        '/v1/alarms',
        'post',
        415,
        'application/problem+json',
      );
      expect(response.json<ProblemResponse>()).toMatchObject({
        title: 'Unsupported Media Type',
        code: 'UNSUPPORTED_MEDIA_TYPE',
        instance: '/v1/alarms',
      });
    }

    await expect(
      pool.query<{ count: number }>('SELECT count(*)::integer AS count FROM alarms'),
    ).resolves.toMatchObject({ rows: before.rows });
  });

  it('returns and schema-validates the documented 500 problem for an unexpected application error', async () => {
    const service = application.get<AlarmApplicationService>(ALARM_APPLICATION_SERVICE);
    const sensitiveFailure = 'phone=010-1234-5678 body=must-not-leak';
    const loggerError = vi.spyOn(Logger.prototype, 'error').mockImplementation(() => undefined);
    const list = vi.spyOn(service, 'list').mockRejectedValueOnce(new Error(sensitiveFailure));
    try {
      const response = await application.inject({ method: 'GET', url: '/v1/alarms' });
      expectDocumentedResponse(
        openApiDocument,
        response,
        '/v1/alarms',
        'get',
        500,
        'application/problem+json',
      );
      const problem = response.json<ProblemResponse>();
      expect(problem).toMatchObject({
        type: 'about:blank',
        title: 'Internal Server Error',
        status: 500,
        code: 'INTERNAL_ERROR',
        detail: 'The server could not complete the request',
        instance: '/v1/alarms',
        traceId: expect.any(String),
      });
      expect(loggerError).toHaveBeenCalledWith({
        eventName: 'UNHANDLED_REQUEST_ERROR',
        traceId: problem.traceId,
        errorName: 'Error',
      });
      expect(JSON.stringify(loggerError.mock.calls)).not.toContain(sensitiveFailure);
    } finally {
      list.mockRestore();
      loggerError.mockRestore();
    }
  });

  it('returns exact command conflicts without mutating terminal alarms', async () => {
    const created = await application.inject({
      method: 'POST',
      url: '/v1/alarms',
      payload: { type: 'BULK', title: 'terminal-command-fixture', body: 'body' },
    });
    const alarm = created.json<AlarmResponse>();
    const startConflictStatuses = [
      'CANCEL_REQUESTED',
      'CANCELED',
      'COMPLETED',
      'PARTIALLY_FAILED',
      'FAILED',
      'ATTENTION_REQUIRED',
    ] as const;
    const cancelConflictStatuses = [
      'COMPLETED',
      'PARTIALLY_FAILED',
      'FAILED',
      'ATTENTION_REQUIRED',
    ] as const;

    const expectConflictWithoutMutation = async (
      action: 'send' | 'cancel',
      status: (typeof startConflictStatuses)[number],
    ): Promise<void> => {
      await pool.query(
        `
          UPDATE alarms
          SET status = $2, updated_at = '2099-01-01T00:00:00.000Z', version = 41
          WHERE id = $1
        `,
        [alarm.id, status],
      );
      const before = await pool.query<{
        status: string;
        version: string;
        updated_at: string;
        job_count: number;
        delivery_count: number;
      }>(
        `
          SELECT
            a.status,
            a.version::text,
            a.updated_at::text,
            (SELECT count(*)::integer FROM alarm_expansion_jobs j WHERE j.alarm_id = a.id)
              AS job_count,
            (SELECT count(*)::integer FROM deliveries d WHERE d.alarm_id = a.id)
              AS delivery_count
          FROM alarms a
          WHERE a.id = $1
        `,
        [alarm.id],
      );
      const response = await application.inject({
        method: 'POST',
        url: `/v1/alarms/${alarm.id}/${action}`,
      });

      expect(response.statusCode).toBe(409);
      expectDocumentedResponse(
        openApiDocument,
        response,
        action === 'send' ? '/v1/alarms/{alarmId}/send' : '/v1/alarms/{alarmId}/cancel',
        'post',
        409,
        'application/problem+json',
      );
      expect(response.headers['content-type']).toContain('application/problem+json');
      expect(response.json<ProblemResponse>()).toMatchObject({
        type: 'about:blank',
        title: 'Conflict',
        status: 409,
        code: 'ALARM_STATE_CONFLICT',
        instance: `/v1/alarms/${alarm.id}/${action}`,
        traceId: expect.any(String),
      });
      await expect(
        pool.query(
          `
            SELECT
              a.status,
              a.version::text,
              a.updated_at::text,
              (SELECT count(*)::integer FROM alarm_expansion_jobs j WHERE j.alarm_id = a.id)
                AS job_count,
              (SELECT count(*)::integer FROM deliveries d WHERE d.alarm_id = a.id)
                AS delivery_count
            FROM alarms a
            WHERE a.id = $1
          `,
          [alarm.id],
        ),
      ).resolves.toMatchObject({ rows: before.rows });
    };

    for (const status of startConflictStatuses) {
      await expectConflictWithoutMutation('send', status);
    }
    for (const status of cancelConflictStatuses) {
      await expectConflictWithoutMutation('cancel', status);
    }
  });

  it('returns the default 50-item page in exact keyset order with the minimum response fields', async () => {
    await pool.query(`
      INSERT INTO alarms(id, type, title, body, status, created_at, updated_at)
      SELECT
        ('10000000-0000-4000-8000-' || lpad(sequence::text, 12, '0'))::uuid,
        'BULK',
        'default-page-' || sequence,
        'body-' || sequence,
        'CREATED',
        '2099-12-31T23:59:59Z'::timestamptz + sequence * interval '1 microsecond',
        '2099-12-31T23:59:59Z'::timestamptz + sequence * interval '1 microsecond'
      FROM generate_series(1, 51) AS sequence
    `);

    try {
      const firstResponse = await application.inject({ method: 'GET', url: '/v1/alarms' });
      const firstPage = firstResponse.json<{
        alarms: AlarmResponse[];
        nextCursor: string | null;
      }>();

      expect(firstResponse.statusCode).toBe(200);
      expect(Object.keys(firstPage).sort()).toEqual(['alarms', 'nextCursor']);
      expect(firstPage.alarms).toHaveLength(50);
      expect(firstPage.nextCursor).toEqual(expect.any(String));
      expect(firstPage.alarms.map(({ id }) => id)).toEqual(
        Array.from(
          { length: 50 },
          (_, index) => `10000000-0000-4000-8000-${(51 - index).toString().padStart(12, '0')}`,
        ),
      );
      const alarmKeys = [
        'active',
        'audienceFinalizedAt',
        'body',
        'canceled',
        'createdAt',
        'duplicateCount',
        'expansionCompletedAt',
        'failed',
        'failureCode',
        'finishedAt',
        'id',
        'incidentCount',
        'incidentSummary',
        'materializedCount',
        'sent',
        'status',
        'title',
        'total',
        'type',
        'unresolved',
        'updatedAt',
      ];
      for (const item of firstPage.alarms) {
        expect(Object.keys(item).sort()).toEqual(alarmKeys);
        expect(item).toMatchObject({
          id: expect.any(String),
          title: expect.stringMatching(/^default-page-/),
          body: expect.stringMatching(/^body-/),
          status: 'CREATED',
        });
      }

      const secondResponse = await application.inject({
        method: 'GET',
        url: `/v1/alarms?cursor=${encodeURIComponent(firstPage.nextCursor ?? '')}`,
      });
      const secondPage = secondResponse.json<{ alarms: AlarmResponse[] }>();
      expect(secondResponse.statusCode).toBe(200);
      expect(secondPage.alarms[0]?.id).toBe('10000000-0000-4000-8000-000000000001');
    } finally {
      await pool.query("DELETE FROM alarms WHERE title LIKE 'default-page-%'");
    }
  });
});
