import { describe, expect, it } from 'vitest'
import { contentViewOf } from './content-views'

describe('contentViewOf', () => {
  it('opens on the rendered page when the URL names no view', () => {
    expect(contentViewOf(null)).toBe('rendered')
    expect(contentViewOf('')).toBe('rendered')
  })

  it('keeps a known view and ignores an unknown one', () => {
    expect(contentViewOf('markdown')).toBe('markdown')
    expect(contentViewOf('text')).toBe('text')
    expect(contentViewOf('html')).toBe('html')
    expect(contentViewOf('links')).toBe('rendered')
  })
})
