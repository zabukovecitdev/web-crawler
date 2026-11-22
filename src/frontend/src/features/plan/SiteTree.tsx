import { ErrorState, LoadingRows } from '@/components/shared/States'
import { TreeRow } from './TreeRow'
import { TreeToolbar } from './TreeToolbar'
import type { RowView } from './row-view'
import type { TreeFilter } from './tree-rows'

type Props = {
  rows: RowView[]
  loading: boolean
  error: Error | null
  query: string
  filter: TreeFilter
  allOpen: boolean
  selectable: boolean
  progressMode: boolean
  onQuery: (query: string) => void
  onFilter: (filter: TreeFilter) => void
  onSelectAll: () => void
  onClear: () => void
  onExpandToggle: () => void
  onToggleOpen: (prefix: string) => void
  onSetChecked: (row: RowView, checked: boolean) => void
  onRetry: () => void
}

function Head({ progressMode }: { progressMode: boolean }) {
  return (
    <div aria-hidden="true" className="hidden grid-cols-[minmax(0,1fr)_120px_110px_110px_170px] gap-x-4 border-b px-3 pb-2 md:grid">
      <span className="label">Path</span>
      <span className="label">{progressMode ? 'Progress' : 'Ticked'}</span>
      <span className="label text-right">Pages</span>
      <span className="label text-right">{progressMode ? '' : 'Updated'}</span>
      <span className="label text-right">Status</span>
    </div>
  )
}

function List({ props }: { props: Props }) {
  if (props.error) {
    return <ErrorState error={props.error} onRetry={props.onRetry} />
  }
  if (props.loading) {
    return <LoadingRows rows={5} />
  }
  if (props.rows.length === 0) {
    return <p className="m-0 py-6 text-center text-[14px] text-muted-foreground">No path matches. Clear the search or show all pages.</p>
  }
  return (
    <ul aria-label="Site map" className="m-0 list-none p-0">
      {props.rows.map((row) => (
        <TreeRow key={row.key} row={row} onToggleOpen={props.onToggleOpen} onSetChecked={props.onSetChecked} />
      ))}
    </ul>
  )
}

export function SiteTree(props: Props) {
  return (
    <div className="flex min-w-0 flex-col gap-4">
      <TreeToolbar
        query={props.query}
        filter={props.filter}
        allOpen={props.allOpen}
        selectable={props.selectable}
        onQuery={props.onQuery}
        onFilter={props.onFilter}
        onSelectAll={props.onSelectAll}
        onClear={props.onClear}
        onExpandToggle={props.onExpandToggle}
      />
      <div className="flex flex-col gap-2">
        <Head progressMode={props.progressMode} />
        <List props={props} />
      </div>
    </div>
  )
}
