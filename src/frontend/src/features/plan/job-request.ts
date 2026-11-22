import type { CreateCrawlJobRequest, PathRule, RenderMode, SiteCheckDto } from '@/types/api'

export const REVIEW_AFTER_PAGES = 100

export type Choices = {
  rules: PathRule[]
  mode: RenderMode
  linked: boolean
  review: boolean
  hasSitemap: boolean
}

export function buildJobRequest(check: SiteCheckDto, choices: Choices): CreateCrawlJobRequest {
  return {
    seedUrls: [check.startUrl],
    renderMode: choices.mode,
    jsNeeded: check.javaScript.verdict !== 'NotNeeded',
    pathRules: choices.rules,
    seedFromSitemap: choices.hasSitemap,
    followLinkedPages: choices.hasSitemap ? choices.linked : true,
    reviewAfterPages: !choices.hasSitemap && choices.review ? REVIEW_AFTER_PAGES : undefined,
    startImmediately: true,
  }
}
