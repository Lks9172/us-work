import { readFile, stat } from 'node:fs/promises';

const publicDocuments = [
  { path: 'README.md', minimumBytes: 1_000 },
  { path: 'MAP.md', minimumBytes: 500 },
  { path: 'WORKFLOW.md', minimumBytes: 500 },
  { path: 'docs/adr/0001-adopt-validated-repository-scaffold.md', minimumBytes: 1_000 },
  { path: 'docs/architecture/repository-layout.md', minimumBytes: 500 },
  { path: 'docs/architecture/alarm-delivery-design.md', minimumBytes: 5_000 },
  { path: 'docs/requirements-traceability.json', minimumBytes: 10_000 },
  { path: 'openapi.json', minimumBytes: 5_000 },
];

const failures = [];

function isRecord(value) {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function resolveLocalReference(document, reference) {
  if (!reference.startsWith('#/')) {
    throw new Error(`external reference is not allowed: ${reference}`);
  }

  let current = document;
  for (const rawToken of reference.slice(2).split('/')) {
    const token = rawToken.replaceAll('~1', '/').replaceAll('~0', '~');
    if (!isRecord(current) || !Object.hasOwn(current, token)) {
      throw new Error(`dangling reference: ${reference}`);
    }
    current = current[token];
  }

  return current;
}

function collectReferences(value, references = []) {
  if (Array.isArray(value)) {
    for (const item of value) collectReferences(item, references);
    return references;
  }
  if (!isRecord(value)) return references;

  if (typeof value.$ref === 'string') references.push(value.$ref);
  for (const child of Object.values(value)) collectReferences(child, references);
  return references;
}

async function validateOpenApi() {
  let document;
  try {
    document = JSON.parse(await readFile('openapi.json', 'utf8'));
  } catch (error) {
    failures.push(`openapi.json: invalid JSON (${error instanceof Error ? error.message : error})`);
    return;
  }

  if (
    !isRecord(document) ||
    typeof document.openapi !== 'string' ||
    !/^3\.0\.\d+$/.test(document.openapi)
  ) {
    failures.push('openapi.json: expected an OpenAPI 3.0.x document');
    return;
  }
  if (!isRecord(document.paths) || !isRecord(document.components)) {
    failures.push('openapi.json: paths and components objects are required');
    return;
  }

  for (const reference of collectReferences(document)) {
    try {
      resolveLocalReference(document, reference);
    } catch (error) {
      failures.push(`openapi.json: ${error instanceof Error ? error.message : error}`);
    }
  }

  const operationIds = [];
  const methods = ['get', 'post', 'put', 'patch', 'delete'];
  for (const [path, pathItem] of Object.entries(document.paths)) {
    if (!path.startsWith('/') || !isRecord(pathItem)) {
      failures.push(`openapi.json: invalid path item ${path}`);
      continue;
    }
    for (const method of methods) {
      const operation = pathItem[method];
      if (operation === undefined) continue;
      if (!isRecord(operation) || typeof operation.operationId !== 'string') {
        failures.push(`openapi.json: ${method} ${path} requires operationId`);
        continue;
      }
      if (!isRecord(operation.responses) || Object.keys(operation.responses).length === 0) {
        failures.push(`openapi.json: ${method} ${path} requires responses`);
      }
      operationIds.push(operation.operationId);
    }
  }
  if (new Set(operationIds).size !== operationIds.length) {
    failures.push('openapi.json: operationId values must be unique');
  }
}

async function getFileSize(path) {
  try {
    const metadata = await stat(path);

    return metadata.isFile() ? metadata.size : null;
  } catch {
    return null;
  }
}

for (const document of publicDocuments) {
  const size = await getFileSize(document.path);

  if (size === null || size < document.minimumBytes) {
    failures.push(`${document.path}: expected at least ${document.minimumBytes} bytes`);
  }
}

await validateOpenApi();

if (failures.length > 0) {
  throw new Error(`Documentation contract failed:\n- ${failures.join('\n- ')}`);
}

console.log(`Validated ${publicDocuments.length} public documents.`);
