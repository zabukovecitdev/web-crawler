import { useQuery } from '@tanstack/react-query'
import { apiFetch, queryString } from '@/lib/api'
import type {
  AnalyticsOverview,
  AnalyticsRange,
  ContentTypeCount,
  HostAnalytics,
  HostLinkEdge,
  StatusClassCounts,
  TimeseriesPoint,
} from '@/types/api'

export type Bucket = 'hour' | 'day'

export function bucketFor(range: AnalyticsRange): Bucket {
  return range === '24h' ? 'hour' : 'day'
}

const get = <T>(path: string, params: Record<string, string | undefined>) =>
  apiFetch<T>(`/api/v1/analytics/${path}${queryString(params)}`)

export function useOverview(range: AnalyticsRange) {
  return useQuery({
    queryKey: ['analytics', 'overview', range],
    queryFn: () => get<AnalyticsOverview>('overview', { range }),
  })
}

export function useHostAnalytics(range: AnalyticsRange) {
  return useQuery({
    queryKey: ['analytics', 'hosts', range],
    queryFn: () => get<HostAnalytics[]>('hosts', { range }),
  })
}

export function useHostLinks(range: AnalyticsRange, host: string | undefined, enabled: boolean) {
  return useQuery({
    queryKey: ['analytics', 'host-links', range, host ?? null],
    queryFn: () => get<HostLinkEdge[]>('host-links', { range, host }),
    enabled,
  })
}

export function useStatusCodes(range: AnalyticsRange, host?: string) {
  return useQuery({
    queryKey: ['analytics', 'status-codes', range, host ?? null],
    queryFn: () => get<StatusClassCounts>('status-codes', { range, host }),
  })
}

export function useContentTypes(range: AnalyticsRange, host?: string) {
  return useQuery({
    queryKey: ['analytics', 'content-types', range, host ?? null],
    queryFn: () => get<ContentTypeCount[]>('content-types', { range, host }),
  })
}

export function useTimeseries(range: AnalyticsRange, host?: string) {
  const bucket = bucketFor(range)
  return useQuery({
    queryKey: ['analytics', 'timeseries', range, host ?? null, bucket],
    queryFn: () => get<TimeseriesPoint[]>('timeseries', { range, host, bucket }),
  })
}
