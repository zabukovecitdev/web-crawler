import { useCallback, useState } from 'react'

const KEY = 'samobot-theme'

export type Theme = 'light' | 'dark'

function readTheme(): Theme {
  return document.documentElement.classList.contains('dark') ? 'dark' : 'light'
}

/** Light/dark via the `dark` class on <html>; remembered per browser. index.html applies it before paint. */
export function useTheme(): { theme: Theme; toggle: () => void } {
  const [theme, setTheme] = useState<Theme>(readTheme)

  const toggle = useCallback(() => {
    const next: Theme = readTheme() === 'dark' ? 'light' : 'dark'
    document.documentElement.classList.toggle('dark', next === 'dark')
    try {
      localStorage.setItem(KEY, next)
    } catch {
      // storage can be blocked; the choice then lasts for this page only
    }
    setTheme(next)
  }, [])

  return { theme, toggle }
}
