import type { AlarmStatus, AlarmType } from './alarm';

export type StartDecision = 'INITIALIZE_BULK' | 'INITIALIZE_EMERGENCY' | 'NOOP' | 'CONFLICT';
export type CancelDecision = 'CANCEL_CREATED' | 'REQUEST_CANCEL' | 'NOOP' | 'CONFLICT';

export function decideStart(type: AlarmType, status: AlarmStatus): StartDecision {
  if (status === 'CREATED') {
    return type === 'BULK' ? 'INITIALIZE_BULK' : 'INITIALIZE_EMERGENCY';
  }

  if (status === 'QUEUED' || status === 'EXPANDING' || status === 'SENDING') {
    return 'NOOP';
  }

  return 'CONFLICT';
}

export function decideCancel(status: AlarmStatus): CancelDecision {
  if (status === 'CREATED') {
    return 'CANCEL_CREATED';
  }

  if (status === 'QUEUED' || status === 'EXPANDING' || status === 'SENDING') {
    return 'REQUEST_CANCEL';
  }

  if (status === 'CANCEL_REQUESTED' || status === 'CANCELED') {
    return 'NOOP';
  }

  return 'CONFLICT';
}
