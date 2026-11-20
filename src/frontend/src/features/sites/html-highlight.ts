export type TokenKind = 'text' | 'tag' | 'attr' | 'value' | 'comment'
export type Token = { kind: TokenKind; text: string }

const MARKUP = /<!--[\s\S]*?(?:-->|$)|<![^>]*>|<\/?[A-Za-z](?:"[^"]*"|'[^']*'|[^>"'])*>?/g
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

function rawTextEnd(html: string, from: number, name: string): number {
  const close = html.toLowerCase().indexOf(`</${name.toLowerCase()}`, from)
  return close === -1 ? html.length : close
}

function pushText(tokens: Token[], text: string): void {
  if (text) {
    tokens.push({ kind: 'text', text })
  }
}

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

export const HIGHLIGHT_LINE_LIMIT = 6000

export function sourceLines(html: string): Token[][] {
  const text = html.replace(/\r\n?/g, '\n').replace(/\n$/, '')
  const plain = text.split('\n')
  return plain.length > HIGHLIGHT_LINE_LIMIT ? plain.map((line) => [{ kind: 'text', text: line }]) : tokenLines(tokenizeHtml(text))
}
