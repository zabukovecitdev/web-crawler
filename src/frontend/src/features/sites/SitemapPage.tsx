import { useParams, useSearchParams } from 'react-router-dom'
import { RobotsCard } from './RobotsCard'
import { SiteFrame } from './SiteFrame'
import { SitemapCard } from './SitemapCard'

export function SitemapPage() {
  const host = useParams().host ?? ''
  const [search] = useSearchParams()
  return (
    <SiteFrame host={host}>{search.get('tab') === 'robots' ? <RobotsCard key={host} host={host} /> : <SitemapCard key={host} host={host} />}</SiteFrame>
  )
}
