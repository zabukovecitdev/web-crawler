const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

type Parts = { year: number; month: number; day: number }

function partsOf(iso: string): Parts {
  const date = new Date(iso)
  return { year: date.getUTCFullYear(), month: date.getUTCMonth(), day: date.getUTCDate() }
}

export function formatRange(oldest: string | null, newest: string | null): string {
  if (!oldest || !newest) {
    return ''
  }
  const from = partsOf(oldest)
  const to = partsOf(newest)
  if (from.year !== to.year) {
    return `${from.year} – ${to.year}`
  }
  if (from.month !== to.month) {
    return `${MONTHS[from.month]} – ${MONTHS[to.month]} ${to.year}`
  }
  return from.day === to.day ? `${to.day} ${MONTHS[to.month]} ${to.year}` : `${MONTHS[to.month]} ${to.year}`
}
