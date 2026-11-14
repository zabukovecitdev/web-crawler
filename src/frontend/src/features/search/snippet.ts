export type SnippetPart = { text: string; match: boolean }

const ENTITIES: Record<string, string> = {
  '&amp;': '&',
  '&lt;': '<',
  '&gt;': '>',
  '&quot;': '"',
  '&#39;': "'",
}

/** Single pass over exactly five entities, so "&amp;lt;" becomes "&lt;" and not "<". */
export function decodeEntities(text: string): string {
  return text.replace(/&(?:amp|lt|gt|quot|#39);/g, (entity) => ENTITIES[entity] ?? entity)
}

/** Splits on the literal mark tags only; every other character is plain text. */
export function splitSnippet(snippet: string): SnippetPart[] {
  const parts: SnippetPart[] = []
  let match = false
  for (const piece of snippet.split(/(<\/?mark>)/)) {
    if (piece === '<mark>' || piece === '</mark>') {
      match = piece === '<mark>'
    } else if (piece) {
      parts.push({ text: decodeEntities(piece), match })
    }
  }
  return parts
}
