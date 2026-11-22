import { describe, expect, it } from 'vitest'
import { blockerOf, renderHint } from './render-hint'

describe('renderHint', () => {
  it('explains the detection in auto mode', () => {
    expect(renderHint('Auto', 'NotNeeded', true).text).toContain('Detected: not needed')
    expect(renderHint('Auto', 'Needed', true)).toEqual({ text: 'Detected: this site needs JavaScript. Every page is rendered in a browser, about 3 s per page.', tone: 'muted' })
    expect(renderHint('Auto', 'ProbablyNeeded', false).tone).toBe('error')
  })

  it('warns when always has no browser and never meets a site that needs it', () => {
    expect(renderHint('Always', 'NotNeeded', false).tone).toBe('error')
    expect(renderHint('Always', 'NotNeeded', true).tone).toBe('muted')
    expect(renderHint('Never', 'Needed', true).tone).toBe('warn')
    expect(renderHint('Never', 'NotNeeded', false)).toEqual({ text: 'Pages are fetched over HTTP only.', tone: 'muted' })
  })
})

describe('blockerOf', () => {
  it.each([
    ['Always', 'NotNeeded', false, 'always'],
    ['Auto', 'Needed', false, 'site'],
    ['Auto', 'ProbablyNeeded', false, 'site'],
    ['Auto', 'NotNeeded', false, null],
    ['Never', 'Needed', false, null],
    ['Always', 'Needed', true, null],
  ] as const)('%s with %s, browser up %s: %s', (mode, verdict, up, expected) => {
    expect(blockerOf(mode, verdict, up)).toBe(expected)
  })
})
