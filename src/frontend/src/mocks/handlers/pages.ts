import { http, HttpResponse } from 'msw'
import { pageDetail, pageLinks } from '../data/pages'
import { json, notFound, paginate } from '../util'

function htmlOf(title: string): string {
  return `<!doctype html>
<html lang="en">
<head><title>${title}</title></head>
<body>
  <h1>${title}</h1>
  <p>Set the crawl delay for each host in milliseconds.</p>
</body>
</html>
`
}

function markdownOf(title: string): string {
  return `# ${title}

Set the crawl delay for each host in **milliseconds**. See the [crawl guide](https://example.com/docs/crawl) for details.

## What changes

- The delay applies to every request to the host
- Adaptive delay can raise it, never lower it
  - Nested item with \`inline code\`

1. Open the site
2. Edit the host settings

> Keep the delay polite. Hosts that answer slowly need a longer one.

| Setting | Default | Unit |
|---|---|---|
| Delay | 1000 | ms |
| Max delay | 30000 | ms |

![Sample image](${globalThis.location?.origin ?? 'http://localhost'}/vite.svg)

\`\`\`json
{ "delayMs": 1000 }
\`\`\`
`
}

export const pagesHandlers = [
  http.get('*/api/v1/pages/:id', ({ params }) => {
    const page = pageDetail(Number(params.id))
    return page ? json(page) : notFound()
  }),
  http.get('*/api/v1/pages/:id/links', ({ params, request }) => {
    const url = new URL(request.url)
    const direction = url.searchParams.get('direction')
    if (direction !== 'out' && direction !== 'in') {
      return json({ title: 'direction must be out or in', status: 400 }, 400)
    }
    const links = pageLinks(Number(params.id), direction)
    return links ? json(paginate(links, url, 50)) : notFound()
  }),
  http.get('*/api/v1/pages/:id/content', ({ params }) => {
    const page = pageDetail(Number(params.id))
    if (!page?.parsed) {
      return notFound()
    }
    return new HttpResponse(htmlOf(page.title ?? page.path), {
      headers: { 'Content-Type': 'text/plain; charset=utf-8' },
    })
  }),
  http.get('*/api/v1/pages/:id/markdown', ({ params }) => {
    const page = pageDetail(Number(params.id))
    if (!page?.parsed) {
      return notFound()
    }
    return new HttpResponse(markdownOf(page.title ?? page.path), {
      headers: { 'Content-Type': 'text/markdown; charset=utf-8' },
    })
  }),
]
