/** The four ways to read the same stored body of a page. The first is the default. */
export const CONTENT_VIEWS = [
  { value: 'rendered', label: 'Rendered' },
  { value: 'markdown', label: 'Markdown' },
  { value: 'text', label: 'Plain text' },
  { value: 'html', label: 'HTML' },
] as const

export type ContentView = (typeof CONTENT_VIEWS)[number]['value']

export const DEFAULT_CONTENT_VIEW: ContentView = 'rendered'

/** The view named in the URL (?view=), or the default when it is missing or unknown. */
export function contentViewOf(param: string | null): ContentView {
  return CONTENT_VIEWS.find((v) => v.value === param)?.value ?? DEFAULT_CONTENT_VIEW
}
