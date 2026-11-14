import { keepPreviousData, useQuery } from '@tanstack/react-query'
import { apiFetch, apiFetchResponse, queryString } from '@/lib/api'
import type { Page, PageDetail, PageSummary } from '@/types/api'
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

export type PageContent = { text: string; truncated: boolean }

async function fetchContent(id: number): Promise<PageContent> {
  const res = await apiFetchResponse(`/api/v1/pages/${id}/content`, { headers: { Accept: 'text/plain' } })
  return { text: await res.text(), truncated: res.headers.get('X-Content-Truncated') === 'true' }
}

/** The stored body, loaded only when `enabled`. Shown as text, never as HTML. */
export function usePageContent(id: number, enabled: boolean) {
  return useQuery({
    queryKey: ['page-content', id],
    queryFn: () => fetchContent(id),
    enabled,
    retry: retryUnlessMissing,
  })
}
