import { memo } from 'react'
import type { Token, TokenKind } from './html-highlight'

const COLOURS: Record<TokenKind, string> = {
  text: '',
  tag: 'text-signal-ink',
  attr: 'text-amber-ink',
  value: 'text-led-ink',
  comment: 'italic text-ink-faint',
}

function Line({ number, tokens, wrap }: { number: number; tokens: Token[]; wrap: boolean }) {
  return (
    <div className="flex [contain-intrinsic-size:auto_20px] [content-visibility:auto]">
      <span aria-hidden="true" className="w-14 shrink-0 select-none pr-4 text-right text-ink-faint">
        {number}
      </span>
      <code className={wrap ? 'min-w-0 flex-1 whitespace-pre-wrap [overflow-wrap:anywhere]' : 'whitespace-pre'}>
        {tokens.map((token, index) => (
          <span key={index} className={COLOURS[token.kind]}>
            {token.text}
          </span>
        ))}
      </code>
    </div>
  )
}

type Props = { lines: Token[][]; wrap: boolean; complete: boolean }

/** The source, one numbered row for each line, with a mark at the end so the reader knows nothing follows. */
export const HtmlSourceLines = memo(function HtmlSourceLines({ lines, wrap, complete }: Props) {
  return (
    <>
      {lines.map((tokens, index) => (
        <Line key={index} number={index + 1} tokens={tokens} wrap={wrap} />
      ))}
      <p className="m-0 mt-3 select-none border-t pt-3 text-ink-faint">
        {complete ? `End of the document (${lines.length} lines).` : 'The dashboard shows only the first part. Download the file for the rest.'}
      </p>
    </>
  )
})
