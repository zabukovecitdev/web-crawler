import { describe, expect, it } from 'vitest'
import type { PathRule } from '@/types/api'
import { CLEARED, checkState, decide, nextChecked, pathOf, setChecked, withPin } from './path-rules'

const exclude = (prefix: string, exact?: boolean): PathRule => ({ kind: 'Exclude', prefix, exact })
const include = (prefix: string, exact?: boolean): PathRule => ({ kind: 'Include', prefix, exact })
const folder = (prefix: string) => ({ prefix, kind: 'Folder' as const })
const page = (prefix: string) => ({ prefix, kind: 'Page' as const })

describe('decide', () => {
  it('allows every path without rules', () => {
    expect(decide([], '/anything')).toBe(true)
  })

  it.each([
    ['/blog/2016/post', false],
    ['/blog/2016/', false],
    ['/blog/2016', false],
    ['/blog/2017/post', true],
    ['/blog/20160/post', true],
  ])('a folder rule covers %s: %s', (path, allowed) => {
    expect(decide([exclude('/blog/2016/')], path)).toBe(allowed)
  })

  it('lets the longest prefix win', () => {
    const rules = [exclude('/blog/'), include('/blog/2026/'), exclude('/blog/2026/drafts/')]
    expect([decide(rules, '/blog/2020/x'), decide(rules, '/blog/2026/x'), decide(rules, '/blog/2026/drafts/x')]).toEqual([false, true, false])
  })

  it('lets an include beat an exclude of the same prefix', () => {
    expect(decide([exclude('/a/'), include('/a/')], '/a/b')).toBe(true)
  })

  it('lets an exact rule beat a longer folder rule and cover only its own path', () => {
    const rules = [exclude('/guide/install/'), include('/guide/install', true)]
    expect([decide(rules, '/guide/install'), decide(rules, '/guide/install/x'), decide([exclude('/s', true)], '/s/more')]).toEqual([true, false, true])
  })

  it('matches paths with case', () => {
    expect(decide([exclude('/Blog/')], '/blog/x')).toBe(true)
  })
})

describe('checkState', () => {
  it('is checked or unchecked when nothing inside deviates', () => {
    expect(checkState([], folder('/blog/'))).toBe('true')
    expect(checkState([exclude('/blog/')], folder('/blog/'))).toBe('false')
  })

  it('is mixed when a folder inside differs from its parent', () => {
    expect(checkState([exclude('/blog/2016/')], folder('/blog/'))).toBe('mixed')
    expect(checkState([exclude('/blog/'), include('/blog/2026/')], folder('/blog/'))).toBe('mixed')
  })

  it('is not mixed by a rule that agrees with its parent', () => {
    expect(checkState([exclude('/blog/'), exclude('/blog/2016/')], folder('/blog/'))).toBe('false')
  })

  it('is mixed by the pinned start page inside an unticked folder', () => {
    const rules = withPin([exclude('/guide/')], '/guide/install')
    expect(checkState(rules, folder('/guide/'))).toBe('mixed')
    expect(checkState(rules, page('/guide/install'))).toBe('true')
  })

  it('follows the rules for a page and for a rest node', () => {
    expect(checkState([exclude('/a/')], page('/a/b'))).toBe('false')
    expect(checkState([exclude('/a/')], { prefix: '/a/', kind: 'Rest' })).toBe('false')
  })
})

describe('setChecked', () => {
  it('adds an exclude for an unticked folder', () => {
    expect(setChecked([], folder('/blog/'), false, '/')).toEqual([exclude('/blog/')])
  })

  it('drops the rules of the folder and everything inside when it is ticked again', () => {
    const rules = [exclude('/blog/'), include('/blog/2026/'), exclude('/events/')]
    expect(setChecked(rules, folder('/blog/'), true, '/')).toEqual([exclude('/events/')])
  })

  it('clears the rules inside a folder that is unticked as a whole', () => {
    expect(setChecked([exclude('/blog/2016/')], folder('/blog/'), false, '/')).toEqual([exclude('/blog/')])
  })

  it('adds an include inside a folder that stays unticked above it', () => {
    expect(setChecked([exclude('/')], folder('/blog/'), true, '/')).toEqual([exclude('/'), include('/blog/')])
  })

  it('writes an exact rule for a page', () => {
    expect(setChecked([], page('/changelog'), false, '/')).toEqual([exclude('/changelog', true)])
    expect(setChecked([exclude('/changelog', true)], page('/changelog'), true, '/')).toEqual([])
  })

  it('keeps the pinned start page whatever happens above it', () => {
    const rules = setChecked([], folder('/guide/'), false, '/guide/install')
    expect(decide(withPin(rules, '/guide/install'), '/guide/install')).toBe(true)
    expect(decide(withPin(rules, '/guide/install'), '/guide/other')).toBe(false)
  })

  it('unticks everything with the cleared rules except the start page', () => {
    const rules = withPin(CLEARED, '/guide/install')
    expect([decide(rules, '/'), decide(rules, '/blog/x'), decide(rules, '/guide/install')]).toEqual([false, false, true])
  })
})

describe('helpers', () => {
  it('toggles a mixed or unchecked box on and a checked box off', () => {
    expect([nextChecked('mixed'), nextChecked('false'), nextChecked('true')]).toEqual([true, true, false])
  })

  it('reads the path of a URL', () => {
    expect(pathOf('https://a.example/guide/install?x=1')).toBe('/guide/install')
    expect(pathOf('nonsense')).toBe('/')
  })
})
