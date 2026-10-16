import { existsSync, readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';

import { Controller, Get, Header } from '@nestjs/common';

export function resolveOpenApiDocumentPath(startDirectory: string = __dirname): string {
  let directory = startDirectory;
  while (true) {
    const candidate = join(directory, 'openapi.json');
    if (existsSync(candidate)) return candidate;

    const parent = dirname(directory);
    if (parent === directory) break;
    directory = parent;
  }
  throw new Error('openapi.json was not found in the API module ancestry');
}

const openApiDocument: unknown = JSON.parse(readFileSync(resolveOpenApiDocumentPath(), 'utf8'));

const swaggerUi = `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>US Alliance Alarm API</title>
    <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css" />
  </head>
  <body>
    <div id="swagger-ui"></div>
    <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
    <script>
      SwaggerUIBundle({
        url: '/docs/openapi.json',
        dom_id: '#swagger-ui',
        deepLinking: true,
        displayRequestDuration: true,
      });
    </script>
  </body>
</html>`;

@Controller()
export class DocsController {
  @Get('docs')
  @Header('Content-Type', 'text/html; charset=utf-8')
  @Header('Cache-Control', 'no-store')
  public ui(): string {
    return swaggerUi;
  }

  @Get('docs/openapi.json')
  @Header('Cache-Control', 'no-store')
  public json(): unknown {
    return openApiDocument;
  }
}
