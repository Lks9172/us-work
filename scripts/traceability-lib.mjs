import { readdir, readFile, stat } from 'node:fs/promises';
import path from 'node:path';

const SOURCE_PRIORITIES = new Set(['P0-source', 'P0-project']);
const REQUIREMENT_SCOPES = new Set(['runtime', 'documentation', 'submission', 'interview']);
const DESIGN_STATUSES = new Set(['drafted', 'accepted', 'superseded']);
const IMPLEMENTATION_STATUSES = new Set(['not-started', 'partial', 'implemented']);
const VERIFICATION_STATUSES = new Set([
  'not-run',
  'partial',
  'passed',
  'failed',
  'external',
  'dependency-deviation',
  'waived',
]);
const VERIFICATION_TYPES = new Set(['automated', 'runtime-manual', 'manual']);
const EVIDENCE_FRESHNESS = new Set(['current-tree', 'historical-snapshot', 'external']);
const TEST_ID_PATTERN = /^[A-Z][A-Z0-9-]*-\d{2}[A-Z]?$/;
const REQUIREMENT_ID_PATTERN = /^(?:R|O)-[A-Z0-9-]+-\d{2}$/;
const SOURCE_EXTENSIONS = new Set(['.js', '.mjs', '.mts', '.ts']);
const FORBIDDEN_PUBLIC_REFERENCES = [
  '.git/local-worklog',
  'docs/internal',
  'file://',
  '/home/',
  '\\wsl.localhost',
];
const DEFAULT_CANONICAL_TEST_PLAN = 'docs/internal/usall-backend-assignment-test-plan.md';

function isRecord(value) {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function isNonEmptyString(value) {
  return typeof value === 'string' && value.trim().length > 0;
}

function requireStringArray(value, field, failures) {
  if (
    !Array.isArray(value) ||
    value.length === 0 ||
    value.some((item) => !isNonEmptyString(item))
  ) {
    failures.push(`${field}: expected a non-empty string array`);
    return [];
  }

  return value;
}

function validateEnum(value, allowed, field, failures) {
  if (!allowed.has(value)) {
    failures.push(`${field}: invalid value ${JSON.stringify(value)}`);
  }
}

async function pathIsFile(root, relativePath) {
  try {
    const metadata = await stat(path.join(root, relativePath));
    return metadata.isFile();
  } catch {
    return false;
  }
}

async function collectSourceText(root, relativeRoots) {
  const chunks = [];

  async function visit(relativePath) {
    const absolutePath = path.join(root, relativePath);
    let entries;
    try {
      entries = await readdir(absolutePath, { withFileTypes: true });
    } catch {
      return;
    }

    for (const entry of entries) {
      if (entry.name === 'node_modules' || entry.name === 'dist' || entry.name === '.git') continue;
      const child = path.join(relativePath, entry.name);
      if (entry.isDirectory()) {
        await visit(child);
        continue;
      }
      if (!SOURCE_EXTENSIONS.has(path.extname(entry.name))) continue;
      chunks.push(await readFile(path.join(root, child), 'utf8'));
    }
  }

  for (const relativeRoot of relativeRoots) await visit(relativeRoot);
  return chunks.join('\n');
}

function evidencePath(evidenceUri) {
  if (!isNonEmptyString(evidenceUri) || evidenceUri.startsWith('command:')) return null;
  return evidenceUri.split('#', 1)[0];
}

function containsExactTestId(sourceText, testId) {
  const escaped = testId.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  return new RegExp(`(^|[^A-Z0-9-])${escaped}([^A-Z0-9-]|$)`, 'm').test(sourceText);
}

async function readCanonicalP0Catalog(root, relativePath, failures) {
  let source;
  try {
    source = await readFile(path.join(root, relativePath), 'utf8');
  } catch (error) {
    if (error?.code === 'ENOENT') return null;
    failures.push(`canonical catalog: unable to read ${relativePath}`);
    return new Map();
  }

  const catalog = new Map();
  for (const line of source.split(/\r?\n/)) {
    const row = /^\|\s*([A-Z][A-Z0-9-]*-\d{2}[A-Z]?)\s*\|\s*([^|]*)\|/.exec(line);
    if (row === null || !row[2].includes('P0')) continue;

    const testId = row[1];
    const sourcePriority = row[2].includes('P0-프로젝트') ? 'P0-project' : 'P0-source';
    if (catalog.has(testId)) {
      failures.push(`canonical catalog: duplicate P0 test ID ${testId}`);
      continue;
    }
    catalog.set(testId, sourcePriority);
  }

  if (catalog.size === 0) {
    failures.push(`canonical catalog: no P0 test IDs found in ${relativePath}`);
  }
  return catalog;
}

export async function validateTraceabilityManifestWithMetadata(manifest, options) {
  const root = options.root;
  const failures = [];
  const serializedManifest = JSON.stringify(manifest);

  for (const forbidden of FORBIDDEN_PUBLIC_REFERENCES) {
    if (serializedManifest.toLowerCase().includes(forbidden.toLowerCase())) {
      failures.push(`manifest: forbidden public reference ${forbidden}`);
    }
  }

  if (!isRecord(manifest)) {
    return { failures: ['manifest: expected an object'], catalogSource: 'manifest-only' };
  }
  if (manifest.schemaVersion !== 1) failures.push('schemaVersion: expected 1');

  const requirements = Array.isArray(manifest.requirements) ? manifest.requirements : [];
  const testGroups = Array.isArray(manifest.testGroups) ? manifest.testGroups : [];
  if (requirements.length === 0) failures.push('requirements: expected at least one entry');
  if (testGroups.length === 0) failures.push('testGroups: expected at least one entry');

  const requirementsById = new Map();
  for (const [index, requirement] of requirements.entries()) {
    const field = `requirements[${index}]`;
    if (!isRecord(requirement)) {
      failures.push(`${field}: expected an object`);
      continue;
    }
    if (!REQUIREMENT_ID_PATTERN.test(requirement.id ?? '')) {
      failures.push(`${field}.id: invalid requirement ID`);
    } else if (requirementsById.has(requirement.id)) {
      failures.push(`${field}.id: duplicate ${requirement.id}`);
    } else {
      requirementsById.set(requirement.id, requirement);
    }

    if (!isNonEmptyString(requirement.sourceSummary)) {
      failures.push(`${field}.sourceSummary: expected a non-empty string`);
    }
    validateEnum(
      requirement.sourcePriority,
      SOURCE_PRIORITIES,
      `${field}.sourcePriority`,
      failures,
    );
    validateEnum(requirement.scope, REQUIREMENT_SCOPES, `${field}.scope`, failures);
    validateEnum(requirement.designStatus, DESIGN_STATUSES, `${field}.designStatus`, failures);
    validateEnum(
      requirement.implementationStatus,
      IMPLEMENTATION_STATUSES,
      `${field}.implementationStatus`,
      failures,
    );
    validateEnum(
      requirement.verificationStatus,
      VERIFICATION_STATUSES,
      `${field}.verificationStatus`,
      failures,
    );

    for (const property of ['interpretation', 'invariant', 'risk', 'knownLimitation']) {
      if (!isNonEmptyString(requirement[property])) {
        failures.push(`${field}.${property}: expected a non-empty string`);
      }
    }

    requireStringArray(requirement.designDecisionIds, `${field}.designDecisionIds`, failures);
    requireStringArray(requirement.contractRefs, `${field}.contractRefs`, failures);
    const artifacts = requireStringArray(
      requirement.implementationArtifacts,
      `${field}.implementationArtifacts`,
      failures,
    );
    for (const artifact of artifacts) {
      if (path.isAbsolute(artifact) || artifact.includes('..')) {
        failures.push(`${field}.implementationArtifacts: unsafe path ${artifact}`);
      } else if (!(await pathIsFile(root, artifact))) {
        failures.push(`${field}.implementationArtifacts: missing file ${artifact}`);
      }
    }
  }

  const expectedCounts = isRecord(manifest.expectedTestCounts) ? manifest.expectedTestCounts : {};
  const seenTestIds = new Set();
  const manifestPriorityByTestId = new Map();
  const assignedRequirementIds = new Set();
  const testCounts = { 'P0-project': 0, 'P0-source': 0 };
  const automatedGroups = [];

  for (const [index, group] of testGroups.entries()) {
    const field = `testGroups[${index}]`;
    if (!isRecord(group)) {
      failures.push(`${field}: expected an object`);
      continue;
    }
    if (!isNonEmptyString(group.id)) failures.push(`${field}.id: expected a non-empty string`);
    validateEnum(group.sourcePriority, SOURCE_PRIORITIES, `${field}.sourcePriority`, failures);
    validateEnum(group.verificationType, VERIFICATION_TYPES, `${field}.verificationType`, failures);
    validateEnum(
      group.verificationStatus,
      VERIFICATION_STATUSES,
      `${field}.verificationStatus`,
      failures,
    );
    validateEnum(
      group.evidenceFreshness,
      EVIDENCE_FRESHNESS,
      `${field}.evidenceFreshness`,
      failures,
    );

    const ids = requireStringArray(group.ids, `${field}.ids`, failures);
    requireStringArray(group.layers, `${field}.layers`, failures);
    const groupRequirementIds = requireStringArray(
      group.requirementIds,
      `${field}.requirementIds`,
      failures,
    );
    for (const requirementId of groupRequirementIds) {
      if (!requirementsById.has(requirementId)) {
        failures.push(`${field}.requirementIds: unknown requirement ${requirementId}`);
      } else {
        assignedRequirementIds.add(requirementId);
      }
    }

    for (const id of ids) {
      if (!TEST_ID_PATTERN.test(id)) failures.push(`${field}.ids: invalid test ID ${id}`);
      if (seenTestIds.has(id)) failures.push(`${field}.ids: duplicate canonical test ID ${id}`);
      seenTestIds.add(id);
      manifestPriorityByTestId.set(id, group.sourcePriority);
      if (group.sourcePriority in testCounts) testCounts[group.sourcePriority] += 1;
    }

    if (group.verificationType === 'automated') {
      if (!isNonEmptyString(group.ciCommand) || !group.ciCommand.startsWith('pnpm ')) {
        failures.push(`${field}.ciCommand: automated groups require a pnpm command`);
      }
      automatedGroups.push({ field, ids });
    } else {
      for (const property of ['owner', 'evidenceUri', 'blocker']) {
        if (!isNonEmptyString(group[property])) {
          failures.push(`${field}.${property}: non-automated evidence requires a value`);
        }
      }
    }

    if (!isNonEmptyString(group.evidenceUri)) {
      failures.push(`${field}.evidenceUri: expected a public evidence reference`);
    } else {
      const relativeEvidencePath = evidencePath(group.evidenceUri);
      if (relativeEvidencePath && !(await pathIsFile(root, relativeEvidencePath))) {
        failures.push(`${field}.evidenceUri: missing public file ${relativeEvidencePath}`);
      }
    }

    if (group.verificationStatus === 'passed' && group.evidenceFreshness !== 'current-tree') {
      failures.push(`${field}: passed evidence must be explicitly current-tree`);
    }
    if (
      ['not-run', 'partial', 'external', 'dependency-deviation'].includes(
        group.verificationStatus,
      ) &&
      !isNonEmptyString(group.blocker)
    ) {
      failures.push(`${field}.blocker: ${group.verificationStatus} requires an explicit blocker`);
    }
  }

  for (const [requirementId, requirement] of requirementsById) {
    if (!assignedRequirementIds.has(requirementId)) {
      failures.push(`requirements: ${requirementId} has no canonical test assignment`);
    }
    if (requirement.scope === 'runtime' && requirement.implementationStatus === 'implemented') {
      const hasVerification = testGroups.some(
        (group) =>
          isRecord(group) &&
          Array.isArray(group.requirementIds) &&
          group.requirementIds.includes(requirementId),
      );
      if (!hasVerification) {
        failures.push(`requirements: implemented runtime ${requirementId} has no verification`);
      }
    }
  }

  const expectedSource = expectedCounts['P0-source'];
  const expectedProject = expectedCounts['P0-project'];
  const expectedTotal = expectedCounts.total;
  if (testCounts['P0-source'] !== expectedSource) {
    failures.push(
      `expectedTestCounts.P0-source: expected ${expectedSource}, found ${testCounts['P0-source']}`,
    );
  }
  if (testCounts['P0-project'] !== expectedProject) {
    failures.push(
      `expectedTestCounts.P0-project: expected ${expectedProject}, found ${testCounts['P0-project']}`,
    );
  }
  if (seenTestIds.size !== expectedTotal) {
    failures.push(`expectedTestCounts.total: expected ${expectedTotal}, found ${seenTestIds.size}`);
  }

  const canonicalCatalog = await readCanonicalP0Catalog(
    root,
    options.canonicalTestPlanPath ?? DEFAULT_CANONICAL_TEST_PLAN,
    failures,
  );
  const catalogSource = canonicalCatalog === null ? 'manifest-only' : 'internal-catalog';
  if (canonicalCatalog !== null) {
    for (const [testId, sourcePriority] of canonicalCatalog) {
      if (!seenTestIds.has(testId)) {
        failures.push(`canonical catalog: P0 test ${testId} is missing from the manifest`);
        continue;
      }
      const manifestPriority = manifestPriorityByTestId.get(testId);
      if (manifestPriority !== sourcePriority) {
        failures.push(
          `canonical catalog: ${testId} is ${sourcePriority}, manifest declares ${manifestPriority}`,
        );
      }
    }
    for (const testId of seenTestIds) {
      if (!canonicalCatalog.has(testId)) {
        failures.push(`canonical catalog: manifest test ${testId} is not a canonical P0 scenario`);
      }
    }

    const canonicalCounts = { 'P0-project': 0, 'P0-source': 0 };
    for (const priority of canonicalCatalog.values()) canonicalCounts[priority] += 1;
    if (expectedSource !== canonicalCounts['P0-source']) {
      failures.push(
        `expectedTestCounts.P0-source: canonical catalog has ${canonicalCounts['P0-source']}, manifest expects ${expectedSource}`,
      );
    }
    if (expectedProject !== canonicalCounts['P0-project']) {
      failures.push(
        `expectedTestCounts.P0-project: canonical catalog has ${canonicalCounts['P0-project']}, manifest expects ${expectedProject}`,
      );
    }
    if (expectedTotal !== canonicalCatalog.size) {
      failures.push(
        `expectedTestCounts.total: canonical catalog has ${canonicalCatalog.size}, manifest expects ${expectedTotal}`,
      );
    }
  }

  const sourceRoots = requireStringArray(manifest.testSourceRoots, 'testSourceRoots', failures);
  const sourceText = await collectSourceText(root, sourceRoots);
  for (const group of automatedGroups) {
    for (const id of group.ids) {
      if (!containsExactTestId(sourceText, id)) {
        failures.push(`${group.field}: automated test ID ${id} is absent from test sources`);
      }
    }
  }

  return { failures, catalogSource };
}

export async function validateTraceabilityManifest(manifest, options) {
  const { failures } = await validateTraceabilityManifestWithMetadata(manifest, options);
  return failures;
}

export async function loadAndValidateTraceability(options = {}) {
  const root = options.root ?? process.cwd();
  const manifestPath = options.manifestPath ?? 'docs/requirements-traceability.json';
  const manifest = JSON.parse(await readFile(path.join(root, manifestPath), 'utf8'));
  const { failures, catalogSource } = await validateTraceabilityManifestWithMetadata(manifest, {
    root,
    canonicalTestPlanPath: options.canonicalTestPlanPath,
  });
  return { failures, manifest, catalogSource };
}
