import { useCallback, useState } from 'react'

const KEY = 'samobot-theme'

export type Theme = 'light' | 'dark'

function readTheme(): Theme {
  return document.documentElement.classList.contains('dark') ? 'dark' : 'light'
}

function remember(theme: Theme): boolean {
  try {
    localStorage.setItem(KEY, theme)
    return true
  } catch {
    return false
  }
}

export function useTheme(): { theme: Theme; toggle: () => void } {
  const [theme, setTheme] = useState<Theme>(readTheme)

  const toggle = useCallback(() => {
    const next: Theme = readTheme() === 'dark' ? 'light' : 'dark'
    document.documentElement.classList.toggle('dark', next === 'dark')
    remember(next)
    setTheme(next)
  }, [])

  return { theme, toggle }
}
