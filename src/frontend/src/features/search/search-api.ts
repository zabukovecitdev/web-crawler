import { keepPreviousData, useQuery } from '@tanstack/react-query'
import { apiFetch, queryString } from '@/lib/api'
import type { SearchResponse } from '@/types/api'

export const PAGE_SIZE = 10
export const MIN_QUERY_LENGTH = 2

export type SearchParams = { q: string; host: string; page: number }

export function canSearch(q: string): boolean {
  return q.trim().length >= MIN_QUERY_LENGTH
}

export function useSearch({ q, host, page }: SearchParams) {
  const query = q.trim()
  return useQuery({
    queryKey: ['search', query, host, page],
    queryFn: () =>
      apiFetch<SearchResponse>(
        `/api/v1/search${queryString({ q: query, host, limit: PAGE_SIZE, offset: (page - 1) * PAGE_SIZE })}`,
      ),
    enabled: canSearch(query),
    placeholderData: keepPreviousData,
  })
}
