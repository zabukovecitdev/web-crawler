import { StatusBadge } from '@/components/shared/StatusBadge'
import { Card } from '@/components/ui/card'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { formatBytes, formatMs } from '@/lib/format'
import { contentWord, formatClock } from './crawl-format'
import { LOG_LIMIT, pathOf, type LogRow } from './crawl-rows'

function LogLine({ row }: { row: LogRow }) {
  return (
    <TableRow>
      <TableCell className="readout whitespace-nowrap text-muted-foreground">{formatClock(row.at)}</TableCell>
      <TableCell className="readout max-w-[260px] break-all">{pathOf(row.url)}</TableCell>
      <TableCell>
        <StatusBadge code={row.statusCode} />
      </TableCell>
      <TableCell className="readout whitespace-nowrap text-right">{formatMs(row.responseTimeMs)}</TableCell>
      <TableCell className="readout whitespace-nowrap text-right">{formatBytes(row.contentLength)}</TableCell>
      <TableCell className="whitespace-nowrap">{contentWord(row.contentStatus)}</TableCell>
    </TableRow>
  )
}

/** The newest requests first, at most 200. Not a live region: it would read out every request. */
export function RequestLog({ rows }: { rows: LogRow[] }) {
  return (
    <Card className="min-w-0 p-5 shadow-zb-1">
      <p className="label">Newest first</p>
      <h2 className="m-0 mb-3 text-base font-semibold">Requests</h2>
      {rows.length === 0 ? (
        <p className="m-0 py-6 text-[13px] text-muted-foreground">No requests yet. They appear here as they finish.</p>
      ) : (
        <div className="max-h-[640px] overflow-y-auto">
          <Table className="min-w-[640px]">
            <TableHeader>
              <TableRow>
                <TableHead className="label">Time</TableHead>
                <TableHead className="label">URL</TableHead>
                <TableHead className="label">Response</TableHead>
                <TableHead className="label text-right">Request time</TableHead>
                <TableHead className="label text-right">Size</TableHead>
                <TableHead className="label">Content</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {rows.map((row) => (
                <LogLine key={row.key} row={row} />
              ))}
            </TableBody>
          </Table>
        </div>
      )}
      {rows.length >= LOG_LIMIT ? <p className="m-0 mt-3 text-[13px] text-muted-foreground">Showing the last {LOG_LIMIT} requests.</p> : null}
    </Card>
  )
}
