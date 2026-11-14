import { keepPreviousData, useQuery } from '@tanstack/react-query'
import { ApiError, apiFetch, queryString } from '@/lib/api'
import type { SiteDetail, SitesResponse } from '@/types/api'

export type SiteSort = 'host' | 'pages' | 'failed' | 'lastCrawl'
export type SitesQuery = { query: string; sort: SiteSort; limit: number; offset: number }

export const hostPath = (host: string): string => `/api/v1/hosts/${encodeURIComponent(host)}`

export function isNotFound(error: unknown): boolean {
  return error instanceof ApiError && error.status === 404
}

/** Retry transient errors, but never a 404. */
export function retryUnlessMissing(count: number, error: unknown): boolean {
  return !isNotFound(error) && count < 2
}

export function useSites(params: SitesQuery) {
  return useQuery({
    queryKey: ['sites', params],
    queryFn: () => apiFetch<SitesResponse>(`/api/v1/hosts${queryString(params)}`),
    placeholderData: keepPreviousData,
  })
}

export function useSite(host: string) {
  return useQuery({
    queryKey: ['site', host],
    queryFn: () => apiFetch<SiteDetail>(hostPath(host)),
    retry: retryUnlessMissing,
  })
}
