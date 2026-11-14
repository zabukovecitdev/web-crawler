import type { CreateCrawlJobRequest } from '@/types/api'

export type CrawlOptions = {
  maxDepth: string
  maxUrls: string
  respectRobots: boolean
  useJsRendering: boolean
}

export const DEFAULT_OPTIONS: CrawlOptions = { maxDepth: '5', maxUrls: '5000', respectRobots: true, useJsRendering: false }

/** null when the address is fine, else a sentence to show under the field. */
export function validateUrl(value: string): string | null {
  const text = value.trim()
  if (!text) {
    return 'Enter the address of the site.'
  }
  try {
    const url = new URL(text)
    return url.protocol === 'http:' || url.protocol === 'https:' ? null : 'Use an address that starts with http:// or https://.'
  } catch {
    return 'This is not a full web address. Include https://.'
  }
}

function validateWhole(value: string, label: string, min: number): string | null {
  if (value.trim() === '') {
    return null
  }
  const number = Number(value)
  return Number.isInteger(number) && number >= min ? null : `${label} must be a whole number, ${min} or more.`
}

export type OptionErrors = { maxDepth: string | null; maxUrls: string | null }

export function validateOptions(options: CrawlOptions): OptionErrors {
  return {
    maxDepth: validateWhole(options.maxDepth, 'Maximum depth', 0),
    maxUrls: validateWhole(options.maxUrls, 'Maximum pages', 1),
  }
}

function numberOrUndefined(value: string): number | undefined {
  return value.trim() === '' ? undefined : Number(value)
}

export function buildRequest(url: string, options: CrawlOptions): CreateCrawlJobRequest {
  return {
    seedUrls: [url.trim()],
    maxDepth: numberOrUndefined(options.maxDepth),
    maxUrls: numberOrUndefined(options.maxUrls),
    respectRobots: options.respectRobots,
    useJsRendering: options.useJsRendering,
    startImmediately: true,
  }
}
