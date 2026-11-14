import { describe, expect, it } from 'vitest'
import { linkTarget } from './link-target'

describe('linkTarget', () => {
  it('routes crawled pages inside the app using the URL host', () => {
    expect(linkTarget('https://docs.example.com/a', 7)).toEqual({
      kind: 'app',
      to: '/sites/docs.example.com/pages/7',
    })
  })

  it('links uncrawled http(s) URLs externally', () => {
    expect(linkTarget('https://other.org/x', null)).toEqual({ kind: 'external', href: 'https://other.org/x' })
  })

  it('renders other schemes and malformed URLs as text, even with a page id', () => {
    expect(linkTarget('javascript:alert(1)', null)).toEqual({ kind: 'text' })
    expect(linkTarget('javascript:alert(1)', 3)).toEqual({ kind: 'text' })
    expect(linkTarget('not a url', null)).toEqual({ kind: 'text' })
  })
})
