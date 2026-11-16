function urlWithProtocol(url: string | undefined, allowed: readonly string[]): string | undefined {
  if (!url) {
    return undefined
  }
  try {
    return allowed.includes(new URL(url).protocol) ? url : undefined
  } catch {
    return undefined
  }
}

/** The URL when a link to it is safe to show (http, https, mailto), otherwise undefined. */
export function safeHref(url?: string): string | undefined {
  return urlWithProtocol(url, ['http:', 'https:', 'mailto:'])
}

/** The URL when an image from it is safe to load (http, https), otherwise undefined. */
export function safeImageSrc(url?: string): string | undefined {
  return urlWithProtocol(url, ['http:', 'https:'])
}
