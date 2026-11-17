import { useEffect, useState } from 'react'

export type GraphPalette = { ink: string; line: string; faint: string; node: string; signal: string; halo: string; track: string }

const VARS: Record<keyof GraphPalette, string> = {
  ink: '--foreground',
  line: '--muted-foreground',
  faint: '--muted-foreground',
  node: '--chart-1',
  signal: '--primary',
  halo: '--card',
  track: '--muted',
}

/** Canvas cannot use CSS variables, so read the current values from the page. Space separated "r g b" becomes rgb(). */
export function readPalette(): GraphPalette {
  const style = getComputedStyle(document.documentElement)
  const colour = (name: string, alpha = 1) =>
    `rgb(${style.getPropertyValue(name).trim().split(/\s+/).join(' ')} / ${alpha})`
  const palette = Object.fromEntries(Object.entries(VARS).map(([key, name]) => [key, colour(name)])) as GraphPalette
  // Lines are lighter than the arrowheads on them, so the direction stays readable where lines cross.
  return { ...palette, line: colour(VARS.line, 0.45) }
}

/** The palette, read again when the page switches between light and dark (the `dark` class on <html>). */
export function useGraphPalette(): GraphPalette {
  const [palette, setPalette] = useState(readPalette)
  useEffect(() => {
    const observer = new MutationObserver(() => setPalette(readPalette()))
    observer.observe(document.documentElement, { attributes: true, attributeFilter: ['class'] })
    return () => observer.disconnect()
  }, [])
  return palette
}

/** The width of an element, kept current while it resizes. */
export function useElementWidth(ref: React.RefObject<HTMLElement | null>): number {
  const [width, setWidth] = useState(0)
  useEffect(() => {
    const element = ref.current
    if (!element) {
      return undefined
    }
    const observer = new ResizeObserver(([entry]) => setWidth(Math.floor(entry.contentRect.width)))
    observer.observe(element)
    return () => observer.disconnect()
  }, [ref])
  return width
}
