FROM node:24.19.0-alpine AS workspace

WORKDIR /workspace

RUN corepack enable && corepack prepare pnpm@11.22.0 --activate

COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./
COPY apps/api/package.json apps/api/package.json
COPY apps/migrate/package.json apps/migrate/package.json
COPY apps/worker/package.json apps/worker/package.json
COPY packages/alarms/package.json packages/alarms/package.json
COPY packages/platform/package.json packages/platform/package.json

FROM workspace AS production-dependencies

RUN pnpm install --frozen-lockfile --prod \
  && mkdir -p packages/alarms/node_modules packages/platform/node_modules

FROM workspace AS build

RUN pnpm install --frozen-lockfile

COPY . .
RUN pnpm build

FROM node:24.19.0-alpine AS runtime

ENV NODE_ENV=production
WORKDIR /workspace

COPY --from=production-dependencies /workspace/node_modules ./node_modules
COPY --from=production-dependencies /workspace/apps/api/node_modules ./apps/api/node_modules
COPY --from=production-dependencies /workspace/apps/migrate/node_modules ./apps/migrate/node_modules
COPY --from=production-dependencies /workspace/apps/worker/node_modules ./apps/worker/node_modules
COPY --from=production-dependencies /workspace/packages/alarms/node_modules ./packages/alarms/node_modules
COPY --from=production-dependencies /workspace/packages/platform/node_modules ./packages/platform/node_modules

COPY --from=build /workspace/apps/api/package.json ./apps/api/package.json
COPY --from=build /workspace/apps/api/dist ./apps/api/dist
COPY --from=build /workspace/apps/migrate/package.json ./apps/migrate/package.json
COPY --from=build /workspace/apps/migrate/dist ./apps/migrate/dist
COPY --from=build /workspace/apps/worker/package.json ./apps/worker/package.json
COPY --from=build /workspace/apps/worker/dist ./apps/worker/dist
COPY --from=build /workspace/packages/alarms/package.json ./packages/alarms/package.json
COPY --from=build /workspace/packages/alarms/dist ./packages/alarms/dist
COPY --from=build /workspace/packages/platform/package.json ./packages/platform/package.json
COPY --from=build /workspace/packages/platform/dist ./packages/platform/dist
COPY --from=build /workspace/migrations ./migrations
COPY --from=build /workspace/openapi.json ./openapi.json

EXPOSE 3000
USER node
CMD ["node", "apps/api/dist/src/main.js"]
