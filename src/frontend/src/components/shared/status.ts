export type Tone = 'success' | 'neutral' | 'warning' | 'signal'

export type StatusLook = { tone: Tone; word: string; mark: string }

/** HTTP status code -> badge tone, a word and a text mark, so colour is never the only signal. */
export function httpStatusLook(code: number | null | undefined): StatusLook {
  if (code === null || code === undefined || code === 0) {
    return { tone: 'neutral', word: 'No response', mark: '–' }
  }
  if (code < 300) {
    return { tone: 'success', word: 'OK', mark: '✓' }
  }
  if (code < 400) {
    return { tone: 'neutral', word: 'Redirect', mark: '→' }
  }
  if (code < 500) {
    return { tone: 'warning', word: 'Client error', mark: '!' }
  }
  return { tone: 'signal', word: 'Server error', mark: '×' }
}
