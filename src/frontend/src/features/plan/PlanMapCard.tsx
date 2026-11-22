import { Panel } from '../sites/Panel'
import { CLEARED, setChecked } from './path-rules'
import { SiteTree } from './SiteTree'
import type { RowView } from './row-view'
import type { PlanChoices } from './usePlanHooks'
import type { PlanMap } from './usePlanMap'

type Props = { map: PlanMap; choices: PlanChoices; hasSitemap: boolean; sitemapPending: boolean }

function subtitle(hasSitemap: boolean, pending: boolean): string {
  if (hasSitemap) {
    return 'Everything in the sitemap is ticked. Untick what you don’t need.'
  }
  return pending
    ? 'The sitemap is being read. The folders appear here as soon as it is ready.'
    : 'There is no sitemap, so only the start page is known. The crawl follows links from there, and you can review the map after the first 100 pages.'
}

export function PlanMapCard({ map, choices, hasSitemap, sitemapPending }: Props) {
  const onQuery = (query: string) => {
    choices.setQuery(query)
    if (query.trim() !== '') {
      map.tree.expandAll()
    }
  }
  return (
    <Panel title={hasSitemap ? 'Choose what to fetch' : 'What is known so far'} kicker="Site map">
      <p className="-mt-3 m-0 text-[13px] text-muted-foreground">{subtitle(hasSitemap, sitemapPending)}</p>
      <SiteTree
        rows={map.rows}
        loading={map.tree.loading || sitemapPending}
        error={map.tree.error}
        query={choices.query}
        filter={choices.filter}
        allOpen={map.allOpen}
        selectable
        progressMode={false}
        onQuery={onQuery}
        onFilter={choices.setFilter}
        onSelectAll={() => choices.setRules([])}
        onClear={() => choices.setRules(CLEARED)}
        onExpandToggle={map.allOpen ? map.tree.collapseAll : map.tree.expandAll}
        onToggleOpen={map.tree.toggle}
        onSetChecked={(row: RowView, checked) => choices.setRules(setChecked(choices.rules, row.node, checked, map.startPath))}
        onRetry={map.tree.reload}
      />
      <p className="m-0 text-[13px] text-muted-foreground">New pages follow their folder: in a ticked folder they are fetched, in an unticked one they are not.</p>
    </Panel>
  )
}
