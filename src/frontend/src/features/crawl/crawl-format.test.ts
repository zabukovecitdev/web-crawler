import { describe, expect, it } from 'vitest'
import { delaySourceWord } from './crawl-format'

describe('delaySourceWord', () => {
  it.each([
    ['default', 'Default delay'],
    ['robots.txt', 'robots.txt Crawl-delay'],
    ['adaptive', 'Adaptive, the site is slow or failing'],
  ] as const)('names the %s source', (source, word) => {
    expect(delaySourceWord(source)).toBe(word)
  })
})
