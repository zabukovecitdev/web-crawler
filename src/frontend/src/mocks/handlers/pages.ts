import { http, HttpResponse } from 'msw'
import { pageDetail } from '../data/pages'
import { json, notFound } from '../util'

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

export const pagesHandlers = [
  http.get('*/api/v1/pages/:id', ({ params }) => {
    const page = pageDetail(Number(params.id))
    return page ? json(page) : notFound()
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
]
