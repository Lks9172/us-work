import { loadAndValidateTraceability } from './traceability-lib.mjs';

const { failures, manifest, catalogSource } = await loadAndValidateTraceability();

if (failures.length > 0) {
  throw new Error(`Requirements traceability validation failed:\n- ${failures.join('\n- ')}`);
}

const testCount = manifest.testGroups.reduce((total, group) => total + group.ids.length, 0);
const statusCounts = Object.fromEntries(
  [...new Set(manifest.testGroups.map((group) => group.verificationStatus))]
    .sort()
    .map((status) => [
      status,
      manifest.testGroups
        .filter((group) => group.verificationStatus === status)
        .reduce((total, group) => total + group.ids.length, 0),
    ]),
);

console.log(
  `Validated ${manifest.requirements.length} requirements and ${testCount} canonical P0 tests (${JSON.stringify(statusCounts)}); catalogSource=${catalogSource}.`,
);
