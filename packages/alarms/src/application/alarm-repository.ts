import type { AlarmSnapshot, AlarmStatus, AlarmType, ValidatedCreateAlarm } from '../domain/alarm';

export interface AlarmCursorPosition {
  readonly createdAt: string;
  readonly id: string;
}

export interface FindAlarmPageQuery {
  readonly limit: number;
  readonly cursor: AlarmCursorPosition | null;
  readonly status: AlarmStatus | null;
  readonly type: AlarmType | null;
}

export interface AlarmPageSlice {
  readonly alarms: readonly AlarmSnapshot[];
  readonly hasMore: boolean;
  readonly nextCursorPosition: AlarmCursorPosition | null;
}

export interface PersistAlarmInput extends ValidatedCreateAlarm {
  readonly id: string;
}

export interface AlarmRepository {
  create(input: PersistAlarmInput): Promise<AlarmSnapshot>;
  findById(alarmId: string): Promise<AlarmSnapshot | null>;
  findPage(query: FindAlarmPageQuery): Promise<AlarmPageSlice>;
  start(alarmId: string): Promise<AlarmSnapshot>;
  cancel(alarmId: string): Promise<AlarmSnapshot>;
}
