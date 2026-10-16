import {
  Body,
  Controller,
  Get,
  Headers,
  HttpCode,
  Inject,
  Param,
  Post,
  Query,
  Res,
} from '@nestjs/common';

import { AlarmNotFoundError } from '../application/alarm-errors';
import { AlarmApplicationService, type ListAlarmsRequest } from '../application/alarm-service';
import type { AlarmSnapshot, CreateAlarmDraft } from '../domain/alarm';
import { ALARM_APPLICATION_SERVICE } from './alarm-presentation.tokens';
import { HttpInputError } from './http-input-error';

const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const CREATE_FIELDS = new Set(['type', 'title', 'body', 'recipientIds']);

interface HeaderResponse {
  header(name: string, value: string): unknown;
}

function parseAlarmId(value: string): string {
  if (!UUID_PATTERN.test(value)) {
    throw new HttpInputError(400, 'INVALID_REQUEST', 'alarmId must be a UUID');
  }

  return value;
}

function assertJsonContentType(contentType: string | undefined): void {
  const mediaType = contentType?.split(';', 1)[0]?.trim().toLowerCase();
  if (mediaType !== 'application/json') {
    throw new HttpInputError(
      415,
      'UNSUPPORTED_MEDIA_TYPE',
      'Content-Type must be application/json',
    );
  }
}

function parseCreateBody(body: unknown): CreateAlarmDraft {
  if (body === null || typeof body !== 'object' || Array.isArray(body)) {
    throw new HttpInputError(400, 'INVALID_REQUEST', 'JSON body must be an object');
  }

  const record = body as Record<string, unknown>;
  const unknownFields = Object.keys(record).filter((field) => !CREATE_FIELDS.has(field));
  if (unknownFields.length > 0) {
    throw new HttpInputError(
      422,
      'INVALID_ALARM',
      'Request contains unsupported fields',
      unknownFields.map((field) => ({ field, code: 'UNKNOWN_FIELD' })),
    );
  }

  return {
    type: record.type,
    title: record.title,
    body: record.body,
    ...(Object.hasOwn(record, 'recipientIds') ? { recipientIds: record.recipientIds } : {}),
  };
}

function oneQueryValue(value: string | string[] | undefined, field: string): string | undefined {
  if (Array.isArray(value)) {
    throw new HttpInputError(422, 'INVALID_QUERY', `${field} may appear only once`);
  }

  return value;
}

function parseListRequest(query: {
  readonly limit?: string | string[];
  readonly cursor?: string | string[];
  readonly status?: string | string[];
  readonly type?: string | string[];
}): ListAlarmsRequest {
  const rawLimit = oneQueryValue(query.limit, 'limit');
  const cursor = oneQueryValue(query.cursor, 'cursor');
  const status = oneQueryValue(query.status, 'status');
  const type = oneQueryValue(query.type, 'type');

  if (cursor === '') {
    throw new HttpInputError(400, 'INVALID_CURSOR', 'cursor must not be empty');
  }
  if (rawLimit !== undefined && !/^\d+$/.test(rawLimit)) {
    throw new HttpInputError(422, 'INVALID_QUERY', 'limit must be an integer from 1 to 100');
  }

  return {
    ...(rawLimit === undefined ? {} : { limit: Number.parseInt(rawLimit, 10) }),
    ...(cursor === undefined ? {} : { cursor }),
    ...(status === undefined ? {} : { status }),
    ...(type === undefined ? {} : { type }),
  };
}

@Controller('v1/alarms')
export class AlarmsController {
  public constructor(
    @Inject(ALARM_APPLICATION_SERVICE)
    private readonly alarms: AlarmApplicationService,
  ) {}

  @Post()
  public async create(
    @Headers('content-type') contentType: string | undefined,
    @Body() body: unknown,
    @Res({ passthrough: true }) response: HeaderResponse,
  ): Promise<AlarmSnapshot> {
    assertJsonContentType(contentType);
    const alarm = await this.alarms.create(parseCreateBody(body));
    response.header('Location', `/v1/alarms/${alarm.id}`);

    return alarm;
  }

  @Get()
  public list(
    @Query()
    query: {
      readonly limit?: string | string[];
      readonly cursor?: string | string[];
      readonly status?: string | string[];
      readonly type?: string | string[];
    },
  ) {
    return this.alarms.list(parseListRequest(query));
  }

  @Get(':alarmId')
  public async findById(@Param('alarmId') rawAlarmId: string): Promise<AlarmSnapshot> {
    const alarmId = parseAlarmId(rawAlarmId);
    const alarm = await this.alarms.findById(alarmId);
    if (alarm === null) {
      throw new AlarmNotFoundError(alarmId);
    }

    return alarm;
  }

  @Post(':alarmId/send')
  @HttpCode(202)
  public async start(
    @Param('alarmId') rawAlarmId: string,
    @Res({ passthrough: true }) response: HeaderResponse,
  ): Promise<AlarmSnapshot> {
    const alarmId = parseAlarmId(rawAlarmId);
    const alarm = await this.alarms.start(alarmId);
    response.header('Location', `/v1/alarms/${alarm.id}`);

    return alarm;
  }

  @Post(':alarmId/cancel')
  @HttpCode(202)
  public async cancel(
    @Param('alarmId') rawAlarmId: string,
    @Res({ passthrough: true }) response: HeaderResponse,
  ): Promise<AlarmSnapshot> {
    const alarmId = parseAlarmId(rawAlarmId);
    const alarm = await this.alarms.cancel(alarmId);
    response.header('Location', `/v1/alarms/${alarm.id}`);

    return alarm;
  }
}
