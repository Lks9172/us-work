import { readFile } from 'node:fs/promises';

export const HTTP_METHODS = ['get', 'post', 'put', 'patch', 'delete'] as const;
export type OpenApiHttpMethod = (typeof HTTP_METHODS)[number];

export interface OpenApiReference {
  readonly $ref: string;
}

export interface OpenApiSchema {
  readonly $ref?: string;
  readonly type?: 'object' | 'array' | 'string' | 'integer' | 'number' | 'boolean';
  readonly nullable?: boolean;
  readonly enum?: readonly unknown[];
  readonly oneOf?: readonly OpenApiSchema[];
  readonly required?: readonly string[];
  readonly properties?: Readonly<Record<string, OpenApiSchema>>;
  readonly additionalProperties?: boolean | OpenApiSchema;
  readonly items?: OpenApiSchema;
  readonly minimum?: number;
  readonly maximum?: number;
  readonly minLength?: number;
  readonly maxLength?: number;
  readonly minItems?: number;
  readonly maxItems?: number;
  readonly uniqueItems?: boolean;
  readonly pattern?: string;
  readonly format?: string;
  readonly 'x-normalized-minLength'?: number;
  readonly 'x-normalized-maxLength'?: number;
  readonly 'x-normalization'?: 'none' | 'trim';
  readonly 'x-reject-whitespace-only'?: boolean;
  readonly 'x-reject-edge-whitespace'?: boolean;
}

interface OpenApiMediaType {
  readonly schema?: OpenApiSchema;
  readonly examples?: Readonly<Record<string, { readonly value?: unknown }>>;
}

interface OpenApiHeader {
  readonly schema?: OpenApiSchema;
}

export interface OpenApiResponse {
  readonly $ref?: string;
  readonly description?: string;
  readonly headers?: Readonly<Record<string, OpenApiHeader | OpenApiReference>>;
  readonly content?: Readonly<Record<string, OpenApiMediaType>>;
}

export interface OpenApiOperation {
  readonly operationId?: string;
  readonly parameters?: readonly (OpenApiReference | Record<string, unknown>)[];
  readonly requestBody?: {
    readonly required?: boolean;
    readonly content?: Readonly<Record<string, OpenApiMediaType>>;
  };
  readonly responses?: Readonly<Record<string, OpenApiResponse>>;
}

export interface OpenApiDocument {
  readonly openapi: string;
  readonly info: { readonly title: string; readonly version: string };
  readonly paths: Readonly<Record<string, Partial<Record<OpenApiHttpMethod, OpenApiOperation>>>>;
  readonly components: {
    readonly schemas: Readonly<Record<string, OpenApiSchema>>;
    readonly responses: Readonly<Record<string, OpenApiResponse>>;
    readonly parameters: Readonly<Record<string, Record<string, unknown>>>;
    readonly headers: Readonly<Record<string, OpenApiHeader>>;
  };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function decodeJsonPointerToken(token: string): string {
  return token.replaceAll('~1', '/').replaceAll('~0', '~');
}

export function resolveLocalReference<T>(document: OpenApiDocument, reference: string): T {
  if (!reference.startsWith('#/')) {
    throw new Error(`Only local OpenAPI references are supported: ${reference}`);
  }

  let current: unknown = document;
  for (const token of reference.slice(2).split('/').map(decodeJsonPointerToken)) {
    if (!isRecord(current) || !Object.hasOwn(current, token)) {
      throw new Error(`Dangling OpenAPI reference: ${reference}`);
    }
    current = current[token];
  }

  return current as T;
}

export function collectLocalReferences(value: unknown): readonly string[] {
  const references: string[] = [];
  const visit = (candidate: unknown): void => {
    if (Array.isArray(candidate)) {
      for (const item of candidate) visit(item);
      return;
    }
    if (!isRecord(candidate)) return;

    if (typeof candidate.$ref === 'string') references.push(candidate.$ref);
    for (const child of Object.values(candidate)) visit(child);
  };
  visit(value);

  return references;
}

export async function loadOpenApiDocument(): Promise<OpenApiDocument> {
  const source = await readFile(new URL('../../openapi.json', import.meta.url), 'utf8');
  return JSON.parse(source) as OpenApiDocument;
}

function dereferenceSchema(
  document: OpenApiDocument,
  schema: OpenApiSchema,
  references = new Set<string>(),
): OpenApiSchema {
  if (schema.$ref === undefined) return schema;
  if (references.has(schema.$ref)) {
    throw new Error(`Cyclic OpenAPI schema reference: ${schema.$ref}`);
  }

  const nextReferences = new Set(references).add(schema.$ref);
  return dereferenceSchema(
    document,
    resolveLocalReference<OpenApiSchema>(document, schema.$ref),
    nextReferences,
  );
}

function scalarEquals(left: unknown, right: unknown): boolean {
  return Object.is(left, right);
}

function codePointLength(value: string): number {
  return [...value].length;
}

function isRfc3339DateTime(value: string): boolean {
  return (
    /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/.test(value) &&
    !Number.isNaN(Date.parse(value))
  );
}

export function validateOpenApiSchema(
  document: OpenApiDocument,
  sourceSchema: OpenApiSchema,
  value: unknown,
  path = '$',
): readonly string[] {
  const schema = dereferenceSchema(document, sourceSchema);

  if (value === null) {
    return schema.nullable === true ? [] : [`${path}: null is not allowed`];
  }

  if (schema.oneOf !== undefined) {
    const branchResults = schema.oneOf.map((branch) =>
      validateOpenApiSchema(document, branch, value, path),
    );
    const matchingBranches = branchResults.filter((issues) => issues.length === 0).length;
    return matchingBranches === 1
      ? []
      : [`${path}: expected exactly one oneOf branch, matched ${matchingBranches}`];
  }

  const issues: string[] = [];
  if (schema.enum !== undefined && !schema.enum.some((item) => scalarEquals(item, value))) {
    issues.push(`${path}: value is not in enum`);
  }

  if (schema.type === 'string') {
    if (typeof value !== 'string') return [...issues, `${path}: expected string`];
    const length = codePointLength(value);
    if (schema.minLength !== undefined && length < schema.minLength) {
      issues.push(`${path}: expected at least ${schema.minLength} code points`);
    }
    if (schema.maxLength !== undefined && length > schema.maxLength) {
      issues.push(`${path}: expected at most ${schema.maxLength} code points`);
    }

    const normalized = schema['x-normalization'] === 'trim' ? value.trim() : value;
    const normalizedLength = codePointLength(normalized);
    if (
      schema['x-normalized-minLength'] !== undefined &&
      normalizedLength < schema['x-normalized-minLength']
    ) {
      issues.push(
        `${path}: normalized value must contain at least ${schema['x-normalized-minLength']} code points`,
      );
    }
    if (
      schema['x-normalized-maxLength'] !== undefined &&
      normalizedLength > schema['x-normalized-maxLength']
    ) {
      issues.push(
        `${path}: normalized value must contain at most ${schema['x-normalized-maxLength']} code points`,
      );
    }
    if (schema['x-reject-whitespace-only'] === true && value.trim().length === 0) {
      issues.push(`${path}: whitespace-only value is not allowed`);
    }
    if (schema['x-reject-edge-whitespace'] === true && value.trim() !== value) {
      issues.push(`${path}: edge whitespace is not allowed`);
    }
    if (schema.pattern !== undefined && !new RegExp(schema.pattern, 'u').test(value)) {
      issues.push(`${path}: value does not match pattern`);
    }
    if (schema.format === 'uuid' && !UUID_PATTERN.test(value)) {
      issues.push(`${path}: expected UUID`);
    }
    if (schema.format === 'date-time' && !isRfc3339DateTime(value)) {
      issues.push(`${path}: expected RFC 3339 date-time`);
    }
    return issues;
  }

  if (schema.type === 'integer' || schema.type === 'number') {
    if (typeof value !== 'number' || !Number.isFinite(value)) {
      return [...issues, `${path}: expected number`];
    }
    if (schema.type === 'integer' && !Number.isInteger(value)) {
      issues.push(`${path}: expected integer`);
    }
    if (schema.minimum !== undefined && value < schema.minimum) {
      issues.push(`${path}: expected value >= ${schema.minimum}`);
    }
    if (schema.maximum !== undefined && value > schema.maximum) {
      issues.push(`${path}: expected value <= ${schema.maximum}`);
    }
    return issues;
  }

  if (schema.type === 'boolean') {
    if (typeof value !== 'boolean') issues.push(`${path}: expected boolean`);
    return issues;
  }

  if (schema.type === 'array') {
    if (!Array.isArray(value)) return [...issues, `${path}: expected array`];
    if (schema.minItems !== undefined && value.length < schema.minItems) {
      issues.push(`${path}: expected at least ${schema.minItems} items`);
    }
    if (schema.maxItems !== undefined && value.length > schema.maxItems) {
      issues.push(`${path}: expected at most ${schema.maxItems} items`);
    }
    if (schema.uniqueItems === true) {
      const serialized = value.map((item) => JSON.stringify(item));
      if (new Set(serialized).size !== serialized.length) {
        issues.push(`${path}: expected unique items`);
      }
    }
    if (schema.items !== undefined) {
      value.forEach((item, index) => {
        issues.push(
          ...validateOpenApiSchema(
            document,
            schema.items as OpenApiSchema,
            item,
            `${path}[${index}]`,
          ),
        );
      });
    }
    return issues;
  }

  if (schema.type === 'object') {
    if (!isRecord(value)) return [...issues, `${path}: expected object`];
    for (const required of schema.required ?? []) {
      if (!Object.hasOwn(value, required))
        issues.push(`${path}.${required}: required property missing`);
    }
    for (const [key, childValue] of Object.entries(value)) {
      const propertySchema = schema.properties?.[key];
      if (propertySchema !== undefined) {
        issues.push(
          ...validateOpenApiSchema(document, propertySchema, childValue, `${path}.${key}`),
        );
      } else if (schema.additionalProperties === false) {
        issues.push(`${path}.${key}: additional property is not allowed`);
      } else if (isRecord(schema.additionalProperties)) {
        issues.push(
          ...validateOpenApiSchema(
            document,
            schema.additionalProperties as OpenApiSchema,
            childValue,
            `${path}.${key}`,
          ),
        );
      }
    }
    return issues;
  }

  if (schema.type === undefined && schema.enum === undefined) {
    issues.push(`${path}: schema has no supported type, enum, oneOf, or $ref`);
  }
  return issues;
}

const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

export function getOperation(
  document: OpenApiDocument,
  path: string,
  method: OpenApiHttpMethod,
): OpenApiOperation {
  const operation = document.paths[path]?.[method];
  if (operation === undefined) throw new Error(`Missing OpenAPI operation: ${method} ${path}`);
  return operation;
}

export function getResponse(
  document: OpenApiDocument,
  path: string,
  method: OpenApiHttpMethod,
  status: number,
): OpenApiResponse {
  const response = getOperation(document, path, method).responses?.[status.toString()];
  if (response === undefined) {
    throw new Error(`Missing OpenAPI response: ${method} ${path} ${status}`);
  }
  if (response.$ref === undefined) return response;
  return resolveLocalReference<OpenApiResponse>(document, response.$ref);
}

export function getResponseSchema(
  document: OpenApiDocument,
  path: string,
  method: OpenApiHttpMethod,
  status: number,
  mediaType: string,
): OpenApiSchema {
  const schema = getResponse(document, path, method, status).content?.[mediaType]?.schema;
  if (schema === undefined) {
    throw new Error(`Missing response schema: ${method} ${path} ${status} ${mediaType}`);
  }
  return schema;
}

export function validateAlarmProjectionInvariants(value: unknown): readonly string[] {
  if (!isRecord(value) || !isRecord(value.incidentSummary)) {
    return ['$: expected Alarm projection with incidentSummary'];
  }

  const issues: string[] = [];
  const summary = value.incidentSummary;
  if (summary.count !== value.incidentCount) {
    issues.push('$.incidentSummary.count: must equal incidentCount');
  }
  if (Array.isArray(summary.codes)) {
    const codes = summary.codes.filter(isRecord);
    const sum = codes.reduce(
      (total, entry) => total + (typeof entry.count === 'number' ? entry.count : 0),
      0,
    );
    if (sum !== value.incidentCount) {
      issues.push('$.incidentSummary.codes: counts must sum to incidentCount');
    }
    const names = codes
      .map((entry) => entry.code)
      .filter((code): code is string => typeof code === 'string');
    if (names.join('\0') !== [...names].sort().join('\0')) {
      issues.push('$.incidentSummary.codes: codes must be sorted');
    }
  }

  if (value.incidentCount === 0) {
    if (summary.firstDetectedAt !== null || summary.lastDetectedAt !== null) {
      issues.push('$.incidentSummary: zero incidents require null timestamps');
    }
  } else if (summary.firstDetectedAt === null || summary.lastDetectedAt === null) {
    issues.push('$.incidentSummary: positive incidents require timestamps');
  }

  if (typeof value.total === 'number') {
    const counters = [value.sent, value.failed, value.canceled, value.active, value.unresolved];
    if (counters.every((counter) => typeof counter === 'number')) {
      const sum = counters.reduce<number>((total, counter) => total + (counter as number), 0);
      if (sum !== value.total) issues.push('$: result counters must sum to total');
    }
  }

  return issues;
}
