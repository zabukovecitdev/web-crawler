import { cn } from '@/lib/utils'

export function Led({ className }: { className?: string }) {
  return <span aria-hidden="true" className={cn('inline-block size-2 rounded-full bg-led', className)} />
}
