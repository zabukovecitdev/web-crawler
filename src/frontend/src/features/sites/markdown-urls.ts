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

export function safeHref(url?: string): string | undefined {
  return urlWithProtocol(url, ['http:', 'https:', 'mailto:'])
}

export function safeImageSrc(url?: string): string | undefined {
  return urlWithProtocol(url, ['http:', 'https:'])
}
