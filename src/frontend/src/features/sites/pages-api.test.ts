import { describe, expect, it } from 'vitest'
import { pageLinksPath } from './pages-api'

describe('pageLinksPath', () => {
  it('builds the links URL with direction, limit and offset', () => {
    expect(pageLinksPath(12, 'out', 0)).toBe('/api/v1/pages/12/links?direction=out&limit=50&offset=0')
    expect(pageLinksPath(12, 'in', 100)).toBe('/api/v1/pages/12/links?direction=in&limit=50&offset=100')
  })
})
