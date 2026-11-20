export const CONTENT_VIEWS = [
  { value: 'rendered', label: 'Rendered' },
  { value: 'markdown', label: 'Markdown' },
  { value: 'text', label: 'Plain text' },
  { value: 'html', label: 'HTML' },
] as const

export type ContentView = (typeof CONTENT_VIEWS)[number]['value']

export const DEFAULT_CONTENT_VIEW: ContentView = 'rendered'

export function contentViewOf(param: string | null): ContentView {
  return CONTENT_VIEWS.find((v) => v.value === param)?.value ?? DEFAULT_CONTENT_VIEW
}
