import { useParams, useSearchParams } from 'react-router-dom'
import { RobotsCard } from './RobotsCard'
import { SiteFrame } from './SiteFrame'
import { SitemapCard } from './SitemapCard'

/** /sites/:host/sitemap shows the sitemap; ?tab=robots shows robots.txt. */
export function SitemapPage() {
  const host = useParams().host ?? ''
  const [search] = useSearchParams()
  return (
    <SiteFrame host={host}>{search.get('tab') === 'robots' ? <RobotsCard host={host} /> : <SitemapCard host={host} />}</SiteFrame>
  )
}
