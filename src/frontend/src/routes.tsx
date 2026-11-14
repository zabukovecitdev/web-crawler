import { Navigate, Route, Routes } from 'react-router-dom'
import { AppShell } from '@/components/layout/AppShell'
import { AnalyticsPage } from '@/features/analytics/AnalyticsPage'
import { LoginPage } from '@/features/auth/LoginPage'
import { ProtectedRoute } from '@/features/auth/ProtectedRoute'
import { CrawlDetailPage } from '@/features/crawl/CrawlDetailPage'
import { CrawlsPage } from '@/features/crawl/CrawlsPage'
import { SearchPage } from '@/features/search/SearchPage'
import { PageDetailPage } from '@/features/sites/PageDetailPage'
import { SiteDetailPage } from '@/features/sites/SiteDetailPage'
import { SitemapPage } from '@/features/sites/SitemapPage'
import { SitesPage } from '@/features/sites/SitesPage'

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
