import { lazy } from 'react'
import { Navigate, Route, Routes } from 'react-router-dom'
import { AppShell } from '@/components/layout/AppShell'
import { LoginPage } from '@/features/auth/LoginPage'
import { ProtectedRoute } from '@/features/auth/ProtectedRoute'

const AnalyticsPage = lazy(() =>
  import('@/features/analytics/AnalyticsPage').then((m) => ({ default: m.AnalyticsPage })),
)
const CrawlDetailPage = lazy(() =>
  import('@/features/crawl/CrawlDetailPage').then((m) => ({ default: m.CrawlDetailPage })),
)
const CrawlsPage = lazy(() => import('@/features/crawl/CrawlsPage').then((m) => ({ default: m.CrawlsPage })))
const SearchPage = lazy(() => import('@/features/search/SearchPage').then((m) => ({ default: m.SearchPage })))
const PageDetailPage = lazy(() =>
  import('@/features/sites/PageDetailPage').then((m) => ({ default: m.PageDetailPage })),
)
const SiteDetailPage = lazy(() =>
  import('@/features/sites/SiteDetailPage').then((m) => ({ default: m.SiteDetailPage })),
)
const SitemapPage = lazy(() => import('@/features/sites/SitemapPage').then((m) => ({ default: m.SitemapPage })))
const SitesPage = lazy(() => import('@/features/sites/SitesPage').then((m) => ({ default: m.SitesPage })))

export function AppRoutes() {
  return (
    <Routes>
      <Route path="/login" element={<LoginPage />} />
      <Route element={<ProtectedRoute />}>
        <Route element={<AppShell />}>
          <Route path="/" element={<SitesPage />} />
          <Route path="/sites/:host" element={<SiteDetailPage />} />
          <Route path="/sites/:host/pages/:id" element={<PageDetailPage />} />
          <Route path="/sites/:host/sitemap" element={<SitemapPage />} />
          <Route path="/search" element={<SearchPage />} />
          <Route path="/crawls" element={<CrawlsPage />} />
          <Route path="/crawls/:jobId" element={<CrawlDetailPage />} />
          <Route path="/analytics" element={<AnalyticsPage />} />
        </Route>
      </Route>
      <Route path="*" element={<Navigate to="/" replace />} />
    </Routes>
  )
}
