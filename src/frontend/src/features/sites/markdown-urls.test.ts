import { describe, expect, it } from 'vitest'
import { safeHref, safeImageSrc } from './markdown-urls'

describe('safeHref', () => {
  it('keeps http, https and mailto URLs', () => {
    expect(safeHref('https://example.com/a?b=1')).toBe('https://example.com/a?b=1')
    expect(safeHref('http://example.com')).toBe('http://example.com')
    expect(safeHref('mailto:a@example.com')).toBe('mailto:a@example.com')
  })

  it('rejects script and data schemes in any case', () => {
    expect(safeHref('javascript:alert(1)')).toBeUndefined()
    expect(safeHref('JaVaScRiPt:alert(1)')).toBeUndefined()
    expect(safeHref('vbscript:msgbox(1)')).toBeUndefined()
    expect(safeHref('data:text/html,<b>x</b>')).toBeUndefined()
  })

  it('rejects relative, empty and missing input', () => {
    expect(safeHref('/relative/path')).toBeUndefined()
    expect(safeHref('')).toBeUndefined()
    expect(safeHref(undefined)).toBeUndefined()
  })
})

describe('safeImageSrc', () => {
  it('keeps http and https URLs', () => {
    expect(safeImageSrc('https://example.com/a.png')).toBe('https://example.com/a.png')
    expect(safeImageSrc('http://example.com/a.png')).toBe('http://example.com/a.png')
  })

  it('rejects mailto, script and data schemes in any case', () => {
    expect(safeImageSrc('mailto:a@example.com')).toBeUndefined()
    expect(safeImageSrc('javascript:alert(1)')).toBeUndefined()
    expect(safeImageSrc('JaVaScRiPt:alert(1)')).toBeUndefined()
    expect(safeImageSrc('vbscript:msgbox(1)')).toBeUndefined()
    expect(safeImageSrc('data:image/png;base64,AAAA')).toBeUndefined()
  })

  it('rejects relative, empty and missing input', () => {
    expect(safeImageSrc('/a.png')).toBeUndefined()
    expect(safeImageSrc('')).toBeUndefined()
    expect(safeImageSrc(undefined)).toBeUndefined()
  })
})
