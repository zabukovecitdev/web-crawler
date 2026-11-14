import { Link } from 'react-router-dom'
import { Button } from '@/components/ui/button'
import { Card } from '@/components/ui/card'
import { Skeleton } from '@/components/ui/skeleton'
import { formatDate, formatNumber } from '@/lib/format'
import type { SearchHitDto } from '@/types/api'
import { PAGE_SIZE } from './search-api'
import { Snippet } from './Snippet'

function hitPath(url: string): string {
  try {
    const parsed = new URL(url)
    return `${parsed.host}${parsed.pathname}${parsed.search}`
  } catch {
    return url
  }
}

function Hit({ hit }: { hit: SearchHitDto }) {
  return (
    <li className="flex flex-col gap-1.5 border-b py-5 first:pt-0 last:border-b-0 last:pb-0">
      <h3 className="m-0 text-lg font-semibold leading-6">
        <Link className="underline-offset-4 hover:underline" to={`/sites/${hit.host}/pages/${hit.pageId}`}>
          {hit.title || hit.url}
        </Link>
      </h3>
      <p className="readout m-0 break-all text-muted-foreground">{hitPath(hit.url)}</p>
      <Snippet text={hit.snippet} />
      <p className="label">
        Fetched · {formatDate(hit.fetchedAt)} &nbsp; Lang · {hit.language ? hit.language.toUpperCase() : '—'}
      </p>
    </li>
  )
}

export function ResultsSkeleton() {
  return (
    <Card className="flex flex-col gap-6 p-6" role="status" aria-label="Loading results">
      {Array.from({ length: 4 }, (_, i) => (
        <div key={i} className="flex flex-col gap-2">
          <Skeleton className="h-6 w-1/2" />
          <Skeleton className="h-4 w-2/3" />
          <Skeleton className="h-10 w-full" />
        </div>
      ))}
    </Card>
  )
}

type Props = { items: SearchHitDto[]; total: number; page: number; onPage: (page: number) => void }

export function Results({ items, total, page, onPage }: Props) {
  const from = (page - 1) * PAGE_SIZE + 1
  const to = from + items.length - 1
  return (
    <Card className="flex flex-col gap-5 p-6">
      <ol className="m-0 flex list-none flex-col p-0" aria-label="Search results">
        {items.map((hit) => (
          <Hit key={hit.pageId} hit={hit} />
        ))}
      </ol>
      <nav aria-label="Pagination" className="flex flex-wrap items-center justify-between gap-3 border-t pt-4">
        <p className="readout m-0 text-muted-foreground">
          {formatNumber(from)}–{formatNumber(to)} of {formatNumber(total)}
        </p>
        <div className="flex items-center gap-2">
          <Button variant="outline" size="sm" disabled={page <= 1} onClick={() => onPage(page - 1)}>
            Previous
          </Button>
          <Button variant="outline" size="sm" disabled={to >= total} onClick={() => onPage(page + 1)}>
            Next
          </Button>
        </div>
      </nav>
    </Card>
  )
}
