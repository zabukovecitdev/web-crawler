import { externalHref } from './external-url'

export type LinkTarget =
  | { kind: 'app'; to: string }
  | { kind: 'external'; href: string }
  | { kind: 'text' }

function hostOf(url: string): string | null {
  try {
    return new URL(url).host || null
  } catch {
    return null
  }
}

/** In-app route for a crawled page, an external link for http(s), otherwise plain text. */
export function linkTarget(url: string, pageId: number | null): LinkTarget {
  const href = externalHref(url)
  if (href === null) {
    return { kind: 'text' }
  }
  const host = pageId === null ? null : hostOf(url)
  return host === null
    ? { kind: 'external', href }
    : { kind: 'app', to: `/sites/${encodeURIComponent(host)}/pages/${pageId}` }
}
