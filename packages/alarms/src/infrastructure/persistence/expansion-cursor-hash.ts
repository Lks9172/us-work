import { createHash } from 'node:crypto';

function sha256(value: string): string {
  return createHash('sha256').update(value).digest('hex');
}

export const FIRST_EXPANSION_PAGE_HASH = sha256('expansion:first-page\0');

export function expansionCursorHash(cursor: string): string {
  return sha256(`expansion:cursor\0${cursor}`);
}
