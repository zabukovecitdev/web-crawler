import { HttpResponse } from 'msw'

export const MINUTE = 60_000
export const HOUR = 60 * MINUTE

/** ISO timestamp `ms` milliseconds before now. */
export function ago(ms: number): string {
  return new Date(Date.now() - ms).toISOString()
}

export function json<T>(body: T, status = 200): Response {
  return HttpResponse.json(body as never, { status })
}

export function notFound(): Response {
  return HttpResponse.json({ title: 'Not found', status: 404 }, { status: 404 })
}

export function noContent(): Response {
  return new HttpResponse(null, { status: 204 })
}

export function paginate<T>(items: T[], url: URL, fallbackLimit = 25): { total: number; items: T[] } {
  const limit = Number(url.searchParams.get('limit') ?? fallbackLimit)
  const offset = Number(url.searchParams.get('offset') ?? 0)
  return { total: items.length, items: items.slice(offset, offset + limit) }
}
