import * as React from "react"

import { useIsMobile } from "@/hooks/use-mobile"
import {
  SIDEBAR_COOKIE_MAX_AGE,
  SIDEBAR_COOKIE_NAME,
  SIDEBAR_KEYBOARD_SHORTCUT,
  type SidebarContextProps,
} from "@/components/ui/sidebar-context"

type OpenControl = { open?: boolean; onOpenChange?: (open: boolean) => void }

function useOpenState(defaultOpen: boolean, control: OpenControl) {
  const [internal, setInternal] = React.useState(defaultOpen)
  const open = control.open ?? internal
  const { onOpenChange } = control
  const setOpen = React.useCallback(
    (value: boolean | ((value: boolean) => boolean)) => {
      const next = typeof value === "function" ? value(open) : value
      if (onOpenChange) {
        onOpenChange(next)
      } else {
        setInternal(next)
      }
      document.cookie = `${SIDEBAR_COOKIE_NAME}=${next}; path=/; max-age=${SIDEBAR_COOKIE_MAX_AGE}`
    },
    [onOpenChange, open]
  )
  return { open, setOpen }
}

function useShortcut(toggle: () => void) {
  React.useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === SIDEBAR_KEYBOARD_SHORTCUT && (event.metaKey || event.ctrlKey)) {
        event.preventDefault()
        toggle()
      }
    }
    window.addEventListener("keydown", onKeyDown)
    return () => window.removeEventListener("keydown", onKeyDown)
  }, [toggle])
}

export function useSidebarState(
  defaultOpen: boolean,
  control: OpenControl
): SidebarContextProps {
  const isMobile = useIsMobile()
  const [openMobile, setOpenMobile] = React.useState(false)
  const { open, setOpen } = useOpenState(defaultOpen, control)
  const toggleSidebar = React.useCallback(() => {
    return isMobile ? setOpenMobile((value) => !value) : setOpen((value) => !value)
  }, [isMobile, setOpen])
  useShortcut(toggleSidebar)
  const state = open ? "expanded" : "collapsed"
  return React.useMemo(
    () => ({ state, open, setOpen, isMobile, openMobile, setOpenMobile, toggleSidebar }),
    [state, open, setOpen, isMobile, openMobile, toggleSidebar]
  )
}
