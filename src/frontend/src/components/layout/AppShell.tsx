import { Link, NavLink, Outlet, useLocation } from 'react-router-dom'
import { Led } from '@/components/shared/Led'
import {
  Sidebar,
  SidebarContent,
  SidebarFooter,
  SidebarHeader,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarProvider,
} from '@/components/ui/sidebar'
import { NAV_ITEMS, isActive } from './nav'
import { UserBlock } from './UserBlock'
import { useLiveCrawls } from './useLiveCrawls'

function Brand() {
  return (
    <Link to="/" className="flex items-center gap-2 px-2 text-lg font-semibold tracking-tight">
      <i aria-hidden="true" className="size-2.5 rounded-[2px] bg-primary" />
      SamoBot
    </Link>
  )
}

function Nav() {
  const { pathname } = useLocation()
  const live = useLiveCrawls()

  return (
    <SidebarMenu className="flex-row flex-wrap gap-0.5 md:flex-col" aria-label="Main">
      {NAV_ITEMS.map((item) => (
        <SidebarMenuItem key={item.to}>
          <SidebarMenuButton asChild isActive={isActive(item, pathname)} className="h-10 px-3 text-[15px]">
            <NavLink to={item.to} aria-current={isActive(item, pathname) ? 'page' : undefined}>
              <item.icon strokeWidth={1.5} />
              {item.label}
              {item.to === '/crawls' && live > 0 ? (
                <span className="ml-auto inline-flex items-center gap-1.5 font-mono text-[11px]">
                  <Led />
                  {live} live
                </span>
              ) : null}
            </NavLink>
          </SidebarMenuButton>
        </SidebarMenuItem>
      ))}
    </SidebarMenu>
  )
}

/** Graphite sidebar on desktop; stacks above the content on phones. */
export function AppShell() {
  return (
    <SidebarProvider className="flex-wrap items-stretch bg-background">
      <Sidebar
        collapsible="none"
        className="h-auto w-full gap-6 border-r border-sidebar-border p-4 md:sticky md:top-0 md:h-svh md:w-[232px] md:shrink-0"
      >
        <SidebarHeader className="p-0">
          <Brand />
        </SidebarHeader>
        <SidebarContent className="flex-none md:flex-1">
          <Nav />
        </SidebarContent>
        <SidebarFooter className="p-0">
          <UserBlock />
        </SidebarFooter>
      </Sidebar>
      <main className="flex min-w-0 flex-[999_1_560px] flex-col gap-6 px-4 pb-12 pt-6 md:px-10 md:pt-8">
        <Outlet />
      </main>
    </SidebarProvider>
  )
}
