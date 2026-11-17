import { createElement } from 'react'
import { renderToStaticMarkup } from 'react-dom/server'
import { describe, expect, it } from 'vitest'
import { MarkdownRender } from './MarkdownRender'

const render = (markdown: string) => renderToStaticMarkup(createElement(MarkdownRender, { markdown }))

describe('MarkdownRender', () => {
  it('hides YAML front matter and renders the body', () => {
    const html = render('---\ntitle: Vercel Documentation\nrelated:\n  - /docs/connect\n---\n\n# Ship anything\n\nText.\n')

    expect(html).not.toContain('title:')
    expect(html).not.toContain('/docs/connect')
    expect(html).not.toContain('<hr')
    expect(html).toContain('<h1>Ship anything</h1>')
    expect(html).toContain('<p>Text.</p>')
  })

  it('keeps a thematic break that is not at the start of the document', () => {
    const html = render('# Title\n\nAbove.\n\n---\n\nBelow.\n')

    expect(html).toContain('<hr/>')
    expect(html).toContain('<p>Below.</p>')
  })
})
