/** @type {import('dependency-cruiser').IConfiguration} */
module.exports = {
  forbidden: [
    {
      name: 'no-circular',
      severity: 'error',
      from: {},
      to: { circular: true },
    },
    {
      name: 'alarms-domain-is-pure',
      severity: 'error',
      from: { path: '^packages/alarms/src/domain' },
      to: {
        path: [
          '^packages/alarms/src/(application|infrastructure|presentation)',
          '^packages/platform',
          '^node_modules/@nestjs',
          '^node_modules/(pg|redis|ioredis)',
        ],
      },
    },
    {
      name: 'alarms-application-does-not-depend-on-adapters',
      severity: 'error',
      from: { path: '^packages/alarms/src/application' },
      to: {
        path: [
          '^packages/alarms/src/(infrastructure|presentation)',
          '^packages/platform',
          '^node_modules/@nestjs',
          '^node_modules/(pg|redis|ioredis)',
        ],
      },
    },
    {
      name: 'platform-does-not-depend-on-alarms',
      severity: 'error',
      from: { path: '^packages/platform' },
      to: { path: '^packages/alarms' },
    },
  ],
  options: {
    doNotFollow: { path: 'node_modules' },
    tsConfig: { fileName: 'tsconfig.base.json' },
    enhancedResolveOptions: { exportsFields: ['exports'] },
    reporterOptions: { dot: { collapsePattern: 'node_modules/[^/]+' } },
  },
};
