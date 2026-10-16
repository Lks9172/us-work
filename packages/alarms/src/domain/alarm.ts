export const ALARM_TYPES = ['BULK', 'EMERGENCY'] as const;
export type AlarmType = (typeof ALARM_TYPES)[number];

export const ALARM_STATUSES = [
  'CREATED',
  'QUEUED',
  'EXPANDING',
  'SENDING',
  'CANCEL_REQUESTED',
  'CANCELED',
  'COMPLETED',
  'PARTIALLY_FAILED',
  'FAILED',
  'ATTENTION_REQUIRED',
] as const;
export type AlarmStatus = (typeof ALARM_STATUSES)[number];

export const INCIDENT_CODES = [
  'LATE_PROVIDER_RECIPIENT_MISMATCH',
  'LATE_PROVIDER_EVIDENCE_CONFLICT',
  'LATE_DISTINCT_DUPLICATE',
  'LATE_POSITIVE_AFTER_FAILED',
  'LATE_POSITIVE_AFTER_CANCELED',
] as const;
export type IncidentCode = (typeof INCIDENT_CODES)[number];

export interface IncidentCodeCount {
  readonly code: IncidentCode;
  readonly count: number;
}

export interface IncidentSummary {
  readonly count: number;
  readonly codes: readonly IncidentCodeCount[];
  readonly firstDetectedAt: string | null;
  readonly lastDetectedAt: string | null;
}

export interface AlarmSnapshot {
  readonly id: string;
  readonly type: AlarmType;
  readonly title: string;
  readonly body: string;
  readonly status: AlarmStatus;
  readonly failureCode: string | null;
  readonly materializedCount: number;
  readonly total: number | null;
  readonly sent: number;
  readonly failed: number;
  readonly canceled: number;
  readonly active: number;
  readonly unresolved: number;
  readonly duplicateCount: number;
  readonly incidentCount: number;
  readonly incidentSummary: IncidentSummary;
  readonly audienceFinalizedAt: string | null;
  readonly expansionCompletedAt: string | null;
  readonly finishedAt: string | null;
  readonly createdAt: string;
  readonly updatedAt: string;
}

export interface CreateAlarmDraft {
  readonly type: unknown;
  readonly title: unknown;
  readonly body: unknown;
  readonly recipientIds?: unknown;
}

export interface ValidatedCreateAlarm {
  readonly type: AlarmType;
  readonly title: string;
  readonly body: string;
  readonly recipientIds: readonly string[];
}

export interface AlarmValidationIssue {
  readonly field: string;
  readonly code: string;
  readonly message: string;
}

export class AlarmValidationError extends Error {
  public readonly code = 'INVALID_ALARM';

  public constructor(public readonly issues: readonly AlarmValidationIssue[]) {
    super('Alarm input is invalid');
    this.name = 'AlarmValidationError';
  }
}

function codePointLength(value: string): number {
  return [...value].length;
}

export function validateCreateAlarm(draft: CreateAlarmDraft): ValidatedCreateAlarm {
  const issues: AlarmValidationIssue[] = [];
  const type = ALARM_TYPES.includes(draft.type as AlarmType)
    ? (draft.type as AlarmType)
    : undefined;

  if (type === undefined) {
    issues.push({
      field: 'type',
      code: 'INVALID_ENUM',
      message: 'type must be BULK or EMERGENCY',
    });
  }

  const title = typeof draft.title === 'string' ? draft.title.trim() : '';
  const titleLength = codePointLength(title);
  if (typeof draft.title !== 'string' || titleLength < 1 || titleLength > 200) {
    issues.push({
      field: 'title',
      code: 'INVALID_LENGTH',
      message: 'title must contain 1 to 200 Unicode code points after trimming',
    });
  }

  const body = typeof draft.body === 'string' ? draft.body : '';
  const bodyLength = codePointLength(body);
  if (
    typeof draft.body !== 'string' ||
    body.trim().length === 0 ||
    bodyLength < 1 ||
    bodyLength > 5_000
  ) {
    issues.push({
      field: 'body',
      code: 'INVALID_LENGTH',
      message: 'body must contain 1 to 5000 Unicode code points and not be whitespace-only',
    });
  }

  let recipientIds: readonly string[] = [];

  if (type === 'BULK') {
    if (draft.recipientIds !== undefined) {
      issues.push({
        field: 'recipientIds',
        code: 'FORBIDDEN_FIELD',
        message: 'recipientIds is not allowed for BULK alarms',
      });
    }
  } else if (type === 'EMERGENCY') {
    if (!Array.isArray(draft.recipientIds) || draft.recipientIds.length < 1) {
      issues.push({
        field: 'recipientIds',
        code: 'REQUIRED_FIELD',
        message: 'EMERGENCY alarms require 1 to 100 recipientIds',
      });
    } else if (draft.recipientIds.length > 100) {
      issues.push({
        field: 'recipientIds',
        code: 'TOO_MANY_ITEMS',
        message: 'EMERGENCY alarms support at most 100 recipientIds',
      });
    } else {
      const seen = new Set<string>();
      const validatedRecipients: string[] = [];

      for (const [index, candidate] of draft.recipientIds.entries()) {
        const field = `recipientIds[${index}]`;
        if (
          typeof candidate !== 'string' ||
          codePointLength(candidate) < 1 ||
          codePointLength(candidate) > 128 ||
          candidate.trim() !== candidate
        ) {
          issues.push({
            field,
            code: 'INVALID_RECIPIENT',
            message:
              'recipient ID must be an exact 1 to 128 code point string without edge whitespace',
          });
          continue;
        }

        if (seen.has(candidate)) {
          issues.push({
            field,
            code: 'DUPLICATE_RECIPIENT',
            message: 'recipient IDs must be unique without normalization',
          });
          continue;
        }

        seen.add(candidate);
        validatedRecipients.push(candidate);
      }

      recipientIds = validatedRecipients;
    }
  }

  if (issues.length > 0 || type === undefined) {
    throw new AlarmValidationError(issues);
  }

  return { type, title, body, recipientIds };
}
