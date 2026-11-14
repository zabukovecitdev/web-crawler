import { describe, expect, it } from 'vitest'
import { decodeEntities, splitSnippet } from './snippet'

describe('decodeEntities', () => {
  it('decodes the five entities', () => {
    expect(decodeEntities('&amp; &lt; &gt; &quot; &#39;')).toBe(`& < > " '`)
  })

  it('decodes once, not twice', () => {
    expect(decodeEntities('&amp;lt;')).toBe('&lt;')
  })

  it('leaves other entities alone', () => {
    expect(decodeEntities('&nbsp;&copy;')).toBe('&nbsp;&copy;')
  })
})

describe('splitSnippet', () => {
  it('splits on mark tags', () => {
    expect(splitSnippet('a <mark>b</mark> c')).toEqual([
      { text: 'a ', match: false },
      { text: 'b', match: true },
      { text: ' c', match: false },
    ])
  })

  it('returns plain text as one part', () => {
    expect(splitSnippet('plain')).toEqual([{ text: 'plain', match: false }])
    expect(splitSnippet('')).toEqual([])
  })

  it('treats other tags as plain text', () => {
    expect(splitSnippet('<b>x</b> <script>')).toEqual([{ text: '<b>x</b> <script>', match: false }])
  })

  it('decodes entities inside and outside a match', () => {
    expect(splitSnippet('&lt;<mark>&amp;</mark>')).toEqual([
      { text: '<', match: false },
      { text: '&', match: true },
    ])
  })

  it('does not let a decoded tag become markup', () => {
    expect(splitSnippet('&lt;mark&gt;x&lt;/mark&gt;')).toEqual([{ text: '<mark>x</mark>', match: false }])
  })
})
