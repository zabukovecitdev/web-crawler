export type TokenKind = 'text' | 'tag' | 'attr' | 'value' | 'comment'
export type Token = { kind: TokenKind; text: string }

// A comment, a doctype or declaration, or a whole tag. Quoted attribute values may hold ">".
const MARKUP = /<!--[\s\S]*?(?:-->|$)|<![^>]*>|<\/?[A-Za-z](?:"[^"]*"|'[^']*'|[^>"'])*>?/g
// Inside a tag: the name, then attributes with an optional value, then the closing bracket.
const TAG_PART = /^<\/?[^\s/>]+|\/?>$|=|"[^"]*"|'[^']*'|[^\s=/>"']+|\s+|./g
const RAW_TEXT = /^<(script|style)\b/i

function kindOfPart(part: string, afterEquals: boolean): TokenKind {
  if (part.startsWith('<') || part.endsWith('>')) {
    return 'tag'
  }
  if (afterEquals) {
    return 'value'
  }
  return /^[\s=]/.test(part) ? 'text' : 'attr'
}

function tagTokens(tag: string): Token[] {
  const tokens: Token[] = []
  let afterEquals = false
  for (const part of tag.match(TAG_PART) ?? []) {
    tokens.push({ kind: kindOfPart(part, afterEquals), text: part })
    afterEquals = part === '=' || (afterEquals && /^\s+$/.test(part))
  }
  return tokens
}

function markupTokens(markup: string): Token[] {
  return markup.startsWith('<!--') ? [{ kind: 'comment', text: markup }] : tagTokens(markup)
}

/** The end of the body of a script or style element that starts at `from`: its closing tag, or the end of the text. */
function rawTextEnd(html: string, from: number, name: string): number {
  const close = html.toLowerCase().indexOf(`</${name.toLowerCase()}`, from)
  return close === -1 ? html.length : close
}

function pushText(tokens: Token[], text: string): void {
  if (text) {
    tokens.push({ kind: 'text', text })
  }
}

/** Splits HTML source into coloured tokens. It never changes the text: the tokens joined are the input. */
export function tokenizeHtml(html: string): Token[] {
  const tokens: Token[] = []
  const pattern = new RegExp(MARKUP.source, 'g')
  let position = 0
  for (let match = pattern.exec(html); match; match = pattern.exec(html)) {
    pushText(tokens, html.slice(position, match.index))
    tokens.push(...markupTokens(match[0]))
    position = match.index + match[0].length
    const raw = RAW_TEXT.exec(match[0])
    if (raw) {
      const end = rawTextEnd(html, position, raw[1])
      pushText(tokens, html.slice(position, end))
      position = pattern.lastIndex = end
    }
  }
  pushText(tokens, html.slice(position))
  return tokens
}

/** The tokens of each line. A token that spans lines is cut at the line breaks. */
export function tokenLines(tokens: Token[]): Token[][] {
  const lines: Token[][] = [[]]
  for (const token of tokens) {
    const parts = token.text.split('\n')
    parts.forEach((text, index) => {
      if (index > 0) {
        lines.push([])
      }
      if (text) {
        lines[lines.length - 1].push({ kind: token.kind, text })
      }
    })
  }
  return lines
}

/** Above this many lines the source is shown without colours, to keep the page fast. */
export const HIGHLIGHT_LINE_LIMIT = 6000

export function sourceLines(html: string): Token[][] {
  const text = html.replace(/\r\n?/g, '\n').replace(/\n$/, '')
  const plain = text.split('\n')
  return plain.length > HIGHLIGHT_LINE_LIMIT ? plain.map((line) => [{ kind: 'text', text: line }]) : tokenLines(tokenizeHtml(text))
}
