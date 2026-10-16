import { randomUUID } from 'node:crypto';
import { hostname } from 'node:os';

/**
 * Boot-unique, business-neutral correlation key shared by every adapter in this process.
 *
 * It intentionally contains no Alarm data and is created once by the platform module, so worker
 * lifecycle, dispatch, expansion, reconciliation and maintenance records can be joined without
 * pretending that each sink is a different worker.
 */
export const PROCESS_INSTANCE_ID = `${hostname()}:${randomUUID()}`;
