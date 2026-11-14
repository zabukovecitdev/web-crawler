import { describe, expect, it } from 'vitest'
import { externalHref } from './external-url'

describe('externalHref', () => {
  it('keeps http and https URLs', () => {
    expect(externalHref('https://example.com/a?b=1')).toBe('https://example.com/a?b=1')
    expect(externalHref('http://example.com')).toBe('http://example.com')
  })

  it('rejects other schemes', () => {
    expect(externalHref('javascript:alert(1)')).toBeNull()
    expect(externalHref('data:text/html,<b>x</b>')).toBeNull()
  })

  it('rejects relative and malformed URLs', () => {
    expect(externalHref('/relative/path')).toBeNull()
    expect(externalHref('')).toBeNull()
  })
})
