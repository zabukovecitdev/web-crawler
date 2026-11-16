import { describe, expect, it } from 'vitest'
import { pagePathOf } from './crawl-rows'

describe('pagePathOf', () => {
  it('links a request to the page screen of its URL', () => {
    expect(pagePathOf({ pageId: 12, host: 'docs.example.com', url: 'https://docs.example.com/a' })).toBe('/sites/docs.example.com/pages/12')
  })

  it('takes the host from the URL when the event has none', () => {
    expect(pagePathOf({ pageId: 12, url: 'https://docs.example.com:8443/a' })).toBe('/sites/docs.example.com%3A8443/pages/12')
  })

  it('gives no link for events recorded before they carried the page id', () => {
    expect(pagePathOf({ url: 'https://docs.example.com/a' })).toBeNull()
    expect(pagePathOf({ pageId: 3, url: 'not a url' })).toBeNull()
  })
})
