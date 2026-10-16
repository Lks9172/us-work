import { ArgumentsHost, Catch, type ExceptionFilter, HttpException, Logger } from '@nestjs/common';
import { safeErrorMetadata } from '@usall-backend-assignment/platform';

import {
  AlarmNotFoundError,
  AlarmStateConflictError,
  InvalidCursorError,
  PersistenceUnavailableError,
} from '../application/alarm-errors';
import { AlarmValidationError } from '../domain/alarm';
import { HttpInputError } from './http-input-error';

interface ErrorRequest {
  readonly id?: string;
  readonly url: string;
}

interface ErrorResponse {
  status(code: number): ErrorResponse;
  header(name: string, value: string): ErrorResponse;
  send(body: unknown): void;
}

interface Problem {
  readonly type: string;
  readonly title: string;
  readonly status: number;
  readonly code: string;
  readonly detail: string;
  readonly instance: string;
  readonly traceId: string;
  readonly errors?: readonly object[];
}

function titleForStatus(status: number): string {
  if (status === 400) return 'Bad Request';
  if (status === 404) return 'Not Found';
  if (status === 413) return 'Payload Too Large';
  if (status === 415) return 'Unsupported Media Type';
  if (status === 422) return 'Unprocessable Entity';
  return 'Request Failed';
}

function codeForHttpStatus(status: number): string {
  if (status === 400) return 'INVALID_REQUEST';
  if (status === 413) return 'PAYLOAD_TOO_LARGE';
  if (status === 415) return 'UNSUPPORTED_MEDIA_TYPE';
  return 'HTTP_ERROR';
}

function fastifyClientError(
  exception: unknown,
): { status: number; code: string; detail: string } | null {
  if (exception === null || typeof exception !== 'object') {
    return null;
  }

  const candidate = exception as { statusCode?: unknown; code?: unknown };
  if (
    typeof candidate.statusCode !== 'number' ||
    ![400, 413, 415].includes(candidate.statusCode) ||
    typeof candidate.code !== 'string' ||
    !candidate.code.startsWith('FST_ERR_')
  ) {
    return null;
  }

  if (candidate.statusCode === 413) {
    return {
      status: 413,
      code: 'PAYLOAD_TOO_LARGE',
      detail: 'Request body exceeds the configured size limit',
    };
  }
  if (candidate.statusCode === 415) {
    return {
      status: 415,
      code: 'UNSUPPORTED_MEDIA_TYPE',
      detail: 'Content-Type must be application/json',
    };
  }
  return { status: 400, code: 'INVALID_REQUEST', detail: 'Request body contains malformed JSON' };
}

@Catch()
export class ProblemExceptionFilter implements ExceptionFilter {
  private readonly logger = new Logger(ProblemExceptionFilter.name);

  public catch(exception: unknown, host: ArgumentsHost): void {
    const context = host.switchToHttp();
    const request = context.getRequest<ErrorRequest>();
    const response = context.getResponse<ErrorResponse>();
    const problem = this.toProblem(exception, request);

    if (problem.status >= 500 && !(exception instanceof PersistenceUnavailableError)) {
      this.logger.error({
        eventName: 'UNHANDLED_REQUEST_ERROR',
        traceId: problem.traceId,
        ...safeErrorMetadata(exception),
      });
    }

    response
      .status(problem.status)
      .header('Content-Type', 'application/problem+json')
      .send(problem);
  }

  private toProblem(exception: unknown, request: ErrorRequest): Problem {
    const base = {
      instance: request.url,
      traceId: request.id ?? 'unavailable',
    };

    if (exception instanceof HttpInputError) {
      return {
        ...base,
        type: 'about:blank',
        title: titleForStatus(exception.status),
        status: exception.status,
        code: exception.code,
        detail: exception.message,
        ...(exception.errors === undefined ? {} : { errors: exception.errors }),
      };
    }
    if (exception instanceof AlarmValidationError) {
      return {
        ...base,
        type: 'about:blank',
        title: 'Unprocessable Entity',
        status: 422,
        code: exception.code,
        detail: exception.message,
        errors: exception.issues,
      };
    }
    if (exception instanceof InvalidCursorError) {
      return {
        ...base,
        type: 'about:blank',
        title: 'Bad Request',
        status: 400,
        code: exception.code,
        detail: exception.message,
      };
    }
    if (exception instanceof AlarmNotFoundError) {
      return {
        ...base,
        type: 'about:blank',
        title: 'Not Found',
        status: 404,
        code: exception.code,
        detail: exception.message,
      };
    }
    if (exception instanceof AlarmStateConflictError) {
      return {
        ...base,
        type: 'about:blank',
        title: 'Conflict',
        status: 409,
        code: exception.code,
        detail: exception.message,
      };
    }
    if (exception instanceof PersistenceUnavailableError) {
      return {
        ...base,
        type: 'about:blank',
        title: 'Service Unavailable',
        status: 503,
        code: exception.code,
        detail: exception.message,
      };
    }
    if (exception instanceof RangeError) {
      return {
        ...base,
        type: 'about:blank',
        title: 'Unprocessable Entity',
        status: 422,
        code: 'INVALID_QUERY',
        detail: exception.message,
      };
    }
    const fastifyError = fastifyClientError(exception);
    if (fastifyError !== null) {
      return {
        ...base,
        type: 'about:blank',
        title: titleForStatus(fastifyError.status),
        status: fastifyError.status,
        code: fastifyError.code,
        detail: fastifyError.detail,
      };
    }
    if (exception instanceof HttpException) {
      const status = exception.getStatus();
      return {
        ...base,
        type: 'about:blank',
        title: titleForStatus(status),
        status,
        code: codeForHttpStatus(status),
        detail: exception.message,
      };
    }

    return {
      ...base,
      type: 'about:blank',
      title: 'Internal Server Error',
      status: 500,
      code: 'INTERNAL_ERROR',
      detail: 'The server could not complete the request',
    };
  }
}
