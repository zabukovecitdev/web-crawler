import { keepPreviousData, useQuery } from '@tanstack/react-query'
import { apiFetch, apiFetchResponse, queryString } from '@/lib/api'
import type { Page, PageDetail, PageLinkDirection, PageLinkDto, PageSummary } from '@/types/api'
import { hostPath, retryUnlessMissing } from './sites-api'

export type PageState = 'all' | 'ok' | 'redirect' | 'error'
export type PagesQuery = { query: string; state: PageState; limit: number; offset: number }

export function useHostPages(host: string, params: PagesQuery) {
  return useQuery({
    queryKey: ['host-pages', host, params],
    queryFn: () => apiFetch<Page<PageSummary>>(`${hostPath(host)}/pages${queryString(params)}`),
    placeholderData: keepPreviousData,
  })
}

export function usePage(id: number, enabled = true) {
  return useQuery({
    queryKey: ['page', id],
    queryFn: () => apiFetch<PageDetail>(`/api/v1/pages/${id}`),
    enabled,
    retry: retryUnlessMissing,
  })
}

/** storedBytes: the size of the whole stored body, or null when the server did not say. */
export type PageContent = { text: string; truncated: boolean; storedBytes: number | null }

async function fetchStoredText(path: string, accept: string): Promise<PageContent> {
  const res = await apiFetchResponse(path, { headers: { Accept: accept } })
  const stored = Number(res.headers.get('X-Content-Stored-Bytes') ?? NaN)
  return { text: await res.text(), truncated: res.headers.get('X-Content-Truncated') === 'true', storedBytes: Number.isFinite(stored) ? stored : null }
}

/** The stored body, loaded only when `enabled`. Shown as text, never as HTML. */
export function usePageContent(id: number, enabled: boolean) {
  return useQuery({
    queryKey: ['page-content', id],
    queryFn: () => fetchStoredText(`/api/v1/pages/${id}/content`, 'text/plain'),
    enabled,
    retry: retryUnlessMissing,
  })
}

/** The stored body converted to Markdown by the API, loaded only when `enabled`. */
export function usePageMarkdown(id: number, enabled: boolean) {
  return useQuery({
    queryKey: ['page-markdown', id],
    queryFn: () => fetchStoredText(`/api/v1/pages/${id}/markdown`, 'text/markdown'),
    enabled,
    retry: retryUnlessMissing,
  })
}

export const LINKS_PAGE_SIZE = 50

export function pageLinksPath(id: number, direction: PageLinkDirection, offset: number): string {
  return `/api/v1/pages/${id}/links${queryString({ direction, limit: LINKS_PAGE_SIZE, offset })}`
}

/** One page of links on this page (out) or to this page (in). */
export function usePageLinks(id: number, direction: PageLinkDirection, offset: number) {
  return useQuery({
    queryKey: ['page-links', id, direction, offset],
    queryFn: () => apiFetch<Page<PageLinkDto>>(pageLinksPath(id, direction, offset)),
    placeholderData: keepPreviousData,
    retry: retryUnlessMissing,
  })
}
