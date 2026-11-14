import { keepPreviousData, useMutation, useQuery } from '@tanstack/react-query'
import { apiFetch, queryString } from '@/lib/api'
import type { ExplainDto, RobotsDto, SitemapDto } from '@/types/api'
import { hostPath, retryUnlessMissing } from './sites-api'

export function useSitemap(host: string, limit: number, offset: number) {
  return useQuery({
    queryKey: ['sitemap', host, limit, offset],
    queryFn: () => apiFetch<SitemapDto>(`${hostPath(host)}/sitemaps${queryString({ limit, offset })}`),
    placeholderData: keepPreviousData,
    retry: retryUnlessMissing,
  })
}

export function useRobots(host: string) {
  return useQuery({
    queryKey: ['robots', host],
    queryFn: () => apiFetch<RobotsDto>(`${hostPath(host)}/robots`),
    retry: retryUnlessMissing,
  })
}

export function useExplain() {
  return useMutation({
    mutationFn: (url: string) =>
      apiFetch<ExplainDto>('/api/v1/hosts/explain', { method: 'POST', body: JSON.stringify({ url }) }),
  })
}
