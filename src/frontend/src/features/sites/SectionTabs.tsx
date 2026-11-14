import { Link } from 'react-router-dom'
import { cn } from '@/lib/utils'

export type SectionTab = { to: string; label: string; active: boolean }

/** URL-based tabs: real links, aria-current on the open one. */
export function SectionTabs({ tabs, label }: { tabs: SectionTab[]; label: string }) {
  return (
    <nav
      aria-label={label}
      className="flex w-fit max-w-full flex-wrap gap-1 rounded-lg bg-muted p-1 shadow-[var(--shadow-inset)]"
    >
      {tabs.map((tab) => (
        <Link
          key={tab.label}
          to={tab.to}
          aria-current={tab.active ? 'page' : undefined}
          className={cn(
            'rounded-md px-3.5 py-1.5 text-[13px] font-medium leading-[18px] text-muted-foreground no-underline hover:text-foreground',
            tab.active && 'bg-card text-foreground shadow-zb-1',
          )}
        >
          {tab.label}
        </Link>
      ))}
    </nav>
  )
}
