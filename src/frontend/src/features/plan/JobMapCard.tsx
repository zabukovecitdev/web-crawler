import { Panel } from '../sites/Panel'
import { errorMessage } from './plan-api'
import { setChecked } from './path-rules'
import { SiteTree } from './SiteTree'
import type { RowView } from './row-view'
import type { JobMap } from './useJobMap'

export function JobMapCard({ map }: { map: JobMap }) {
  const onQuery = (query: string) => {
    map.setQuery(query)
    if (query.trim() !== '') {
      map.tree.expandAll()
    }
  }
  return (
    <Panel title="Progress by folder" kicker="Site map">
      <p className="-mt-3 m-0 text-[13px] text-muted-foreground">
        {map.editable ? 'Untick a folder to stop fetching it. Pages already fetched are kept.' : 'How far each folder got. This crawl is over, so the folders can’t be changed.'}
      </p>
      <SiteTree
        rows={map.rows}
        loading={map.tree.loading}
        error={map.tree.error}
        query={map.query}
        filter={map.filter}
        allOpen={map.allOpen}
        selectable={false}
        progressMode
        onQuery={onQuery}
        onFilter={map.setFilter}
        onSelectAll={() => undefined}
        onClear={() => undefined}
        onExpandToggle={map.allOpen ? map.tree.collapseAll : map.tree.expandAll}
        onToggleOpen={map.tree.toggle}
        onSetChecked={(row: RowView, checked) => map.setRules(setChecked(map.rules, row.node, checked, map.startPath))}
        onRetry={map.tree.reload}
      />
      {map.saveError ? (
        <p role="alert" className="m-0 text-[13px] text-signal-ink">
          <strong>The change was not saved:</strong> {errorMessage(map.saveError)}
        </p>
      ) : null}
      <p className="m-0 text-[13px] text-muted-foreground">New pages follow their folder: in a ticked folder they are fetched, in an unticked one they are not.</p>
    </Panel>
  )
}
