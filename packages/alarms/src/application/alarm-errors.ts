export class AlarmNotFoundError extends Error {
  public readonly code = 'ALARM_NOT_FOUND';

  public constructor(public readonly alarmId: string) {
    super(`Alarm not found: ${alarmId}`);
    this.name = 'AlarmNotFoundError';
  }
}

export class AlarmStateConflictError extends Error {
  public readonly code = 'ALARM_STATE_CONFLICT';

  public constructor(
    public readonly alarmId: string,
    public readonly action: 'START' | 'CANCEL',
    public readonly status: string,
  ) {
    super(`Cannot ${action.toLowerCase()} alarm ${alarmId} from ${status}`);
    this.name = 'AlarmStateConflictError';
  }
}

export class InvalidCursorError extends Error {
  public readonly code = 'INVALID_CURSOR';

  public constructor() {
    super('Cursor is invalid');
    this.name = 'InvalidCursorError';
  }
}

export class PersistenceUnavailableError extends Error {
  public readonly code = 'PERSISTENCE_UNAVAILABLE';

  public constructor(cause?: unknown) {
    super('Persistence is unavailable', { cause });
    this.name = 'PersistenceUnavailableError';
  }
}
