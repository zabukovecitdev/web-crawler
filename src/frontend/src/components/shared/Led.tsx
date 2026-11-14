import { cn } from '@/lib/utils'

/** The status LED: a mark only. Always put a word next to it. */
export function Led({ className }: { className?: string }) {
  return <span aria-hidden="true" className={cn('inline-block size-2 rounded-full bg-led', className)} />
}
