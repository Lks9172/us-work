import {
  ALARM_STATUSES,
  ALARM_TYPES,
  type AlarmSnapshot,
  type AlarmStatus,
  type AlarmType,
  type CreateAlarmDraft,
  validateCreateAlarm,
} from '../domain/alarm';
import type { AlarmRepository } from './alarm-repository';
import { AlarmCursorCodec } from './cursor-codec';

export interface ListAlarmsRequest {
  readonly limit?: number;
  readonly cursor?: string;
  readonly status?: string;
  readonly type?: string;
}

export interface AlarmList {
  readonly alarms: readonly AlarmSnapshot[];
  readonly nextCursor: string | null;
}

export class AlarmApplicationService {
  private readonly cursorCodec: AlarmCursorCodec;

  public constructor(
    private readonly repository: AlarmRepository,
    cursorSigningKey: string,
    private readonly generateId: () => string,
  ) {
    this.cursorCodec = new AlarmCursorCodec(cursorSigningKey);
  }

  public create(draft: CreateAlarmDraft): Promise<AlarmSnapshot> {
    const validated = validateCreateAlarm(draft);

    return this.repository.create({ id: this.generateId(), ...validated });
  }

  public async list(request: ListAlarmsRequest): Promise<AlarmList> {
    const limit = request.limit ?? 50;
    if (!Number.isInteger(limit) || limit < 1 || limit > 100) {
      throw new RangeError('limit must be an integer from 1 to 100');
    }

    const status = request.status ?? null;
    if (status !== null && !ALARM_STATUSES.includes(status as AlarmStatus)) {
      throw new RangeError('status is invalid');
    }

    const type = request.type ?? null;
    if (type !== null && !ALARM_TYPES.includes(type as AlarmType)) {
      throw new RangeError('type is invalid');
    }

    const page = await this.repository.findPage({
      limit,
      cursor: request.cursor === undefined ? null : this.cursorCodec.decode(request.cursor),
      status: status as AlarmStatus | null,
      type: type as AlarmType | null,
    });

    return {
      alarms: page.alarms,
      nextCursor:
        page.hasMore && page.nextCursorPosition !== null
          ? this.cursorCodec.encode(page.nextCursorPosition)
          : null,
    };
  }

  public findById(alarmId: string): Promise<AlarmSnapshot | null> {
    return this.repository.findById(alarmId);
  }

  public start(alarmId: string): Promise<AlarmSnapshot> {
    return this.repository.start(alarmId);
  }

  public cancel(alarmId: string): Promise<AlarmSnapshot> {
    return this.repository.cancel(alarmId);
  }
}
