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

export type PageContent = { text: string; truncated: boolean; cutAtCrawl: boolean; storedBytes: number | null }

async function fetchStoredText(path: string, accept: string): Promise<PageContent> {
  const res = await apiFetchResponse(path, { headers: { Accept: accept } })
  const stored = Number(res.headers.get('X-Content-Stored-Bytes') ?? NaN)
  return {
    text: await res.text(),
    truncated: res.headers.get('X-Content-Truncated') === 'true',
    cutAtCrawl: res.headers.get('X-Content-Cut-At-Crawl') === 'true',
    storedBytes: Number.isFinite(stored) ? stored : null,
  }
}

export function usePageContent(id: number, enabled: boolean) {
  return useQuery({
    queryKey: ['page-content', id],
    queryFn: () => fetchStoredText(`/api/v1/pages/${id}/content`, 'text/plain'),
    enabled,
    retry: retryUnlessMissing,
  })
}

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

export function usePageLinks(id: number, direction: PageLinkDirection, offset: number) {
  return useQuery({
    queryKey: ['page-links', id, direction, offset],
    queryFn: () => apiFetch<Page<PageLinkDto>>(pageLinksPath(id, direction, offset)),
    placeholderData: keepPreviousData,
    retry: retryUnlessMissing,
  })
}
