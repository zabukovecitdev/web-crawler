import { describe, expect, it } from 'vitest'
import { delaySourceWord, jobLook } from './crawl-format'

describe('delaySourceWord', () => {
  it.each([
    ['default', 'Default delay'],
    ['robots.txt', 'robots.txt Crawl-delay'],
    ['adaptive', 'Adaptive, the site is slow or failing'],
  ] as const)('names the %s source', (source, word) => {
    expect(delaySourceWord(source)).toBe(word)
  })
})

describe('jobLook with a pause reason', () => {
  it('says why a paused crawl waits', () => {
    expect(jobLook('Paused', 'browser-unavailable').word).toBe('Waiting for browser')
    expect(jobLook('Paused', 'review').word).toBe('Waiting for your review')
  })

  it('keeps the plain look for a pause by the user and ignores a reason on a running crawl', () => {
    expect(jobLook('Paused').word).toBe('Paused')
    expect(jobLook('Running', 'review').word).toBe('Crawling')
  })
})
