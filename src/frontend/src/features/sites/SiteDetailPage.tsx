import { useParams } from 'react-router-dom'
import { PagesPanel } from './PagesPanel'
import { SiteFrame } from './SiteFrame'
import { useSite } from './sites-api'

function PagesTab({ host }: { host: string }) {
  const site = useSite(host)
  return site.data ? <PagesPanel host={host} site={site.data} /> : null
}

export function SiteDetailPage() {
  const host = useParams().host ?? ''
  return (
    <SiteFrame host={host}>
      <PagesTab host={host} />
    </SiteFrame>
  )
}
