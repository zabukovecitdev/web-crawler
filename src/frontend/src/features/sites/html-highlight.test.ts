import { describe, expect, it } from 'vitest'
import { sourceLines, tokenizeHtml, tokenLines } from './html-highlight'

const join = (html: string) => tokenizeHtml(html).map((token) => token.text).join('')

describe('tokenizeHtml', () => {
  it('never changes the text', () => {
    const samples = [
      '<!doctype html>\n<html lang="en"><head><title>T</title></head><body class=a data-x=\'1 > 2\'>x &amp; y<br/></body></html>',
      '<p>unclosed <b attr',
      '<!-- never closed',
      '<script>if (a < b) { document.write("<p>") }</script><p>after</p>',
      'a < b and 1 > 0',
      '',
    ]
    for (const html of samples) {
      expect(join(html)).toBe(html)
    }
  })

  it('names tags, attributes and values', () => {
    const tokens = tokenizeHtml('<a href="/x" hidden>go</a>')
    expect(tokens.map((token) => `${token.kind}:${token.text}`)).toEqual(
      ['tag:<a', 'text: ', 'attr:href', 'text:=', 'value:"/x"', 'text: ', 'attr:hidden', 'tag:>', 'text:go', 'tag:</a', 'tag:>'],
    )
  })

  it('keeps a script body as text and a comment whole', () => {
    const tokens = tokenizeHtml('<script>a<b</script><!-- <p> -->')
    expect(tokens.find((token) => token.text === 'a<b')?.kind).toBe('text')
    expect(tokens[tokens.length - 1]).toEqual({ kind: 'comment', text: '<!-- <p> -->' })
  })
})

describe('lines', () => {
  it('cuts a token that spans lines', () => {
    expect(tokenLines([{ kind: 'comment', text: '<!-- a\n\nb -->' }])).toEqual([
      [{ kind: 'comment', text: '<!-- a' }],
      [],
      [{ kind: 'comment', text: 'b -->' }],
    ])
  })

  it('counts lines without a last empty one', () => {
    expect(sourceLines('<p>\r\nx\n</p>\n')).toHaveLength(3)
  })
})
