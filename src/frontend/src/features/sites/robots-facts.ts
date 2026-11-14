import type { RobotsDto } from '@/types/api'

function countLines(content: string, directive: string): number {
  const prefix = `${directive.toLowerCase()}:`
  return content.split('\n').filter((line) => line.trim().toLowerCase().startsWith(prefix)).length
}

export type RobotsFacts = { blockedPaths: number; sitemapsNamed: number }

/** Facts read from the file text. The server stays the source of truth for the crawl delay. */
export function robotsFacts(robots: RobotsDto): RobotsFacts {
  return {
    blockedPaths: countLines(robots.content, 'Disallow'),
    sitemapsNamed: countLines(robots.content, 'Sitemap'),
  }
}
