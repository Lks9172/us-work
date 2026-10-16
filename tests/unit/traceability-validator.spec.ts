import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';

import { afterEach, describe, expect, it } from 'vitest';

import {
  validateTraceabilityManifest,
  validateTraceabilityManifestWithMetadata,
} from '../../scripts/traceability-lib.mjs';

const temporaryRoots: string[] = [];

async function createFixtureRoot(testSource = "it('[TST-01] is executable', () => {});") {
  const root = await mkdtemp(path.join(tmpdir(), 'usall-traceability-'));
  temporaryRoots.push(root);
  await mkdir(path.join(root, 'src'));
  await mkdir(path.join(root, 'tests'));
  await mkdir(path.join(root, 'docs', 'internal'), { recursive: true });
  await writeFile(
    path.join(root, 'src', 'implementation.ts'),
    'export const implemented = true;\n',
  );
  await writeFile(path.join(root, 'tests', 'feature.spec.ts'), testSource);
  await writeFile(
    path.join(root, 'docs', 'internal', 'usall-backend-assignment-test-plan.md'),
    '| ID | level | scenario | result |\n|---|---|---|---|\n| TST-01 | U/P0 | fixture | pass |\n',
  );
  await writeFile(path.join(root, 'README.md'), '# Evidence\n');
  return root;
}

function validManifest() {
  return {
    schemaVersion: 1,
    expectedTestCounts: { 'P0-project': 0, 'P0-source': 1, total: 1 },
    testSourceRoots: ['tests'],
    requirements: [
      {
        id: 'R-TST-01',
        sourceSummary: 'Validate one runtime requirement.',
        sourcePriority: 'P0-source',
        scope: 'runtime',
        interpretation: 'The fixture is deliberately minimal.',
        designDecisionIds: ['ADR-TST'],
        contractRefs: ['README.md'],
        implementationArtifacts: ['src/implementation.ts'],
        invariant: 'The test ID is present in executable source.',
        risk: 'A missing ID would break traceability.',
        designStatus: 'accepted',
        implementationStatus: 'implemented',
        verificationStatus: 'partial',
        knownLimitation: 'This is a validator self-test fixture.',
      },
    ],
    testGroups: [
      {
        id: 'fixture-automated',
        sourcePriority: 'P0-source',
        requirementIds: ['R-TST-01'],
        ids: ['TST-01'],
        layers: ['unit'],
        verificationType: 'automated',
        ciCommand: 'pnpm test:unit',
        verificationStatus: 'partial',
        evidenceFreshness: 'historical-snapshot',
        evidenceUri: 'README.md#evidence',
        blocker: 'The fixture intentionally does not claim a current-tree pass.',
      },
    ],
  };
}

afterEach(async () => {
  await Promise.all(
    temporaryRoots.splice(0).map((root) => rm(root, { force: true, recursive: true })),
  );
});

describe('requirements traceability validator', () => {
  it('accepts a complete manifest whose automated ID occurs in test source', async () => {
    const root = await createFixtureRoot();

    await expect(validateTraceabilityManifest(validManifest(), { root })).resolves.toEqual([]);
  });

  it('uses manifest-only validation when the private canonical catalog is absent', async () => {
    const root = await createFixtureRoot();
    await rm(path.join(root, 'docs', 'internal'), { force: true, recursive: true });

    await expect(
      validateTraceabilityManifestWithMetadata(validManifest(), { root }),
    ).resolves.toEqual({ failures: [], catalogSource: 'manifest-only' });
  });

  it('keeps count, duplicate, and executable-source gates in manifest-only mode', async () => {
    const root = await createFixtureRoot("it('has no traceability ID', () => {});");
    await rm(path.join(root, 'docs', 'internal'), { force: true, recursive: true });
    const manifest = validManifest();
    manifest.testGroups.push({ ...manifest.testGroups[0], id: 'duplicate-fixture' });
    manifest.expectedTestCounts = { 'P0-project': 0, 'P0-source': 1, total: 1 };

    const result = await validateTraceabilityManifestWithMetadata(manifest, { root });

    expect(result.catalogSource).toBe('manifest-only');
    expect(result.failures).toContain('testGroups[1].ids: duplicate canonical test ID TST-01');
    expect(result.failures).toContain('expectedTestCounts.P0-source: expected 1, found 2');
    expect(result.failures).toContain(
      'testGroups[0]: automated test ID TST-01 is absent from test sources',
    );
  });

  it('rejects an automated ID that is absent from executable test source', async () => {
    const root = await createFixtureRoot("it('has no canonical metadata', () => {});");

    const failures = await validateTraceabilityManifest(validManifest(), { root });

    expect(failures).toContain(
      'testGroups[0]: automated test ID TST-01 is absent from test sources',
    );
  });

  it('rejects incomplete non-automated evidence and missing implementation artifacts', async () => {
    const root = await createFixtureRoot();
    const manifest = validManifest();
    manifest.requirements[0].implementationArtifacts = ['src/missing.ts'];
    manifest.testGroups[0] = {
      ...manifest.testGroups[0],
      verificationType: 'manual',
      owner: '',
      evidenceUri: '',
      blocker: '',
    };

    const failures = await validateTraceabilityManifest(manifest, { root });

    expect(failures).toContain(
      'requirements[0].implementationArtifacts: missing file src/missing.ts',
    );
    expect(failures).toContain('testGroups[0].owner: non-automated evidence requires a value');
    expect(failures).toContain(
      'testGroups[0].evidenceUri: non-automated evidence requires a value',
    );
    expect(failures).toContain('testGroups[0].blocker: non-automated evidence requires a value');
  });

  it('rejects invalid status values, duplicate test IDs, and private evidence references', async () => {
    const root = await createFixtureRoot();
    const manifest = validManifest();
    manifest.requirements[0].verificationStatus = 'green';
    manifest.testGroups.push({
      ...manifest.testGroups[0],
      id: 'duplicate-fixture',
      evidenceUri: '.git/local-worklog/private-evidence.json',
    });
    manifest.expectedTestCounts.total = 2;
    manifest.expectedTestCounts['P0-source'] = 2;

    const failures = await validateTraceabilityManifest(manifest, { root });

    expect(failures).toContain('manifest: forbidden public reference .git/local-worklog');
    expect(failures).toContain('requirements[0].verificationStatus: invalid value "green"');
    expect(failures).toContain('testGroups[1].ids: duplicate canonical test ID TST-01');
  });

  it('rejects manifest IDs and priorities that drift from the canonical P0 catalog', async () => {
    const root = await createFixtureRoot(
      "it('[TST-01] is executable', () => {});\nit('[TST-02] is executable', () => {});",
    );
    const manifest = validManifest();
    manifest.testGroups[0].ids = ['TST-02'];

    const validation = await validateTraceabilityManifestWithMetadata(manifest, { root });
    const failures = validation.failures;

    expect(validation.catalogSource).toBe('internal-catalog');

    expect(failures).toContain('canonical catalog: P0 test TST-01 is missing from the manifest');
    expect(failures).toContain(
      'canonical catalog: manifest test TST-02 is not a canonical P0 scenario',
    );

    manifest.testGroups[0].ids = ['TST-01'];
    manifest.testGroups[0].sourcePriority = 'P0-project';
    manifest.expectedTestCounts = { 'P0-project': 1, 'P0-source': 0, total: 1 };
    const priorityFailures = await validateTraceabilityManifest(manifest, { root });
    expect(priorityFailures).toContain(
      'canonical catalog: TST-01 is P0-source, manifest declares P0-project',
    );
  });
});
