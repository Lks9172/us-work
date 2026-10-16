export interface SafeErrorMetadata {
  readonly errorName: string;
  readonly errorCode?: string;
}

const SAFE_ERROR_NAMES = new Set([
  'AbortError',
  'AggregateError',
  'Error',
  'EvalError',
  'RangeError',
  'ReferenceError',
  'SyntaxError',
  'TimeoutError',
  'TypeError',
  'URIError',
]);

const SAFE_ERROR_CODES = new Set([
  '57P01',
  'EAI_AGAIN',
  'ECONNABORTED',
  'ECONNREFUSED',
  'ECONNRESET',
  'EHOSTUNREACH',
  'ENETUNREACH',
  'ENOTFOUND',
  'EPIPE',
  'ETIMEDOUT',
  'UND_ERR_BODY_TIMEOUT',
  'UND_ERR_CONNECT_TIMEOUT',
  'UND_ERR_HEADERS_TIMEOUT',
  'UND_ERR_SOCKET',
]);

function readProperty(error: Error, property: 'name' | 'code'): unknown {
  try {
    return Reflect.get(error, property);
  } catch {
    return undefined;
  }
}

/**
 * Return bounded, allow-listed error metadata for logs.
 *
 * Error messages, stacks, causes, and arbitrary properties may contain request bodies,
 * recipients, connection strings, or upstream payloads, so this function never reads them.
 */
export function safeErrorMetadata(error: unknown): SafeErrorMetadata {
  let isError = false;
  try {
    isError = error instanceof Error;
  } catch {
    return { errorName: 'UnknownError' };
  }

  if (!isError) {
    return { errorName: 'UnknownError' };
  }

  const candidateName = readProperty(error as Error, 'name');
  const errorName =
    typeof candidateName === 'string' && SAFE_ERROR_NAMES.has(candidateName)
      ? candidateName
      : 'Error';
  const candidateCode = readProperty(error as Error, 'code');

  return typeof candidateCode === 'string' && SAFE_ERROR_CODES.has(candidateCode)
    ? { errorName, errorCode: candidateCode }
    : { errorName };
}
