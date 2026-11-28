import { describe, expect, it } from 'vitest'
import { pagePathOf } from './crawl-rows'

describe('pagePathOf', () => {
  it('links a request to the page screen of its URL', () => {
    expect(pagePathOf({ pageId: 12, host: 'docs.example.com' })).toBe('/sites/docs.example.com/pages/12')
  })

  it('encodes a host key with a port', () => {
    expect(pagePathOf({ pageId: 12, host: 'docs.example.com:8443' })).toBe('/sites/docs.example.com%3A8443/pages/12')
  })
})
