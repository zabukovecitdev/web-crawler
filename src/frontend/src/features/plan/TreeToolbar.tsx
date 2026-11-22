import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Segmented } from '@/components/shared/Segmented'
import type { TreeFilter } from './tree-rows'

type Props = {
  query: string
  filter: TreeFilter
  allOpen: boolean
  selectable: boolean
  onQuery: (query: string) => void
  onFilter: (filter: TreeFilter) => void
  onSelectAll: () => void
  onClear: () => void
  onExpandToggle: () => void
}

const FILTERS = [
  { value: 'all', label: 'All' },
  { value: 'ticked', label: 'Ticked' },
  { value: 'unticked', label: 'Unticked' },
] as const

export function TreeToolbar(props: Props) {
  return (
    <div className="flex flex-wrap items-center justify-between gap-3">
      <Input
        type="search"
        aria-label="Find a path"
        placeholder="Find a path, for example /blog/2024/"
        className="w-full sm:w-72"
        value={props.query}
        onChange={(event) => props.onQuery(event.target.value)}
      />
      <div className="flex flex-wrap items-center gap-2">
        <Segmented label="Show" options={[...FILTERS]} value={props.filter} onChange={props.onFilter} />
        {props.selectable ? (
          <>
            <Button variant="ghost" size="sm" onClick={props.onSelectAll}>
              Select all
            </Button>
            <Button variant="ghost" size="sm" onClick={props.onClear}>
              Clear
            </Button>
          </>
        ) : null}
        <Button variant="ghost" size="sm" onClick={props.onExpandToggle}>
          {props.allOpen ? 'Collapse all' : 'Expand all'}
        </Button>
      </div>
    </div>
  )
}
