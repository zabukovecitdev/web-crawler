import { Activity, BarChart3, Globe, Search, type LucideIcon } from 'lucide-react'

export type NavItem = {
  to: string
  label: string
  icon: LucideIcon
  /** Path prefixes that keep this item highlighted. */
  match: string[]
}

export const NAV_ITEMS: NavItem[] = [
  { to: '/', label: 'Sites', icon: Globe, match: ['/sites'] },
  { to: '/search', label: 'Search', icon: Search, match: ['/search'] },
  { to: '/crawls', label: 'Crawls', icon: Activity, match: ['/crawls'] },
  { to: '/analytics', label: 'Analytics', icon: BarChart3, match: ['/analytics'] },
]

export function isActive(item: NavItem, pathname: string): boolean {
  if (item.to === '/' && pathname === '/') {
    return true
  }
  return item.match.some((prefix) => pathname === prefix || pathname.startsWith(`${prefix}/`))
}
