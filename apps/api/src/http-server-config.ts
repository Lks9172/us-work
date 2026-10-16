export const API_BODY_LIMIT_BYTES = 128 * 1024;

export const API_FASTIFY_OPTIONS = {
  bodyLimit: API_BODY_LIMIT_BYTES,
  exposeHeadRoutes: false,
} as const;
