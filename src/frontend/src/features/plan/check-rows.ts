import type { JsVerdict, SiteCheckDto } from '@/types/api'
import { formatNumber } from './plan-summary'

export type CheckTone = 'ok' | 'warn' | 'err' | 'info' | 'pending'

export type CheckRow = { name: string; tone: CheckTone; text: string }

function startPageRow(check: SiteCheckDto): CheckRow {
  const { startPage } = check
  const stays = `The crawl stays on ${check.host}.`
  if (startPage.blockedByRobots) {
    return { name: 'Start page', tone: 'err', text: `robots.txt does not allow SamoBot to fetch ${check.startUrl}.` }
  }
  if (startPage.problem) {
    return { name: 'Start page', tone: 'err', text: `${startPage.problem} Nothing was fetched.` }
  }
  const answer = startPage.reachable ? 'answers 200 OK' : `answers ${startPage.statusCode}`
  const requestedHost = new URL(check.requestedUrl).host
  const lead = check.redirected ? `${requestedHost} redirects to ${check.host}, which ${answer}.` : `${check.host} ${answer}.`
  return { name: 'Start page', tone: startPage.reachable ? 'ok' : 'err', text: `${lead} ${stays}` }
}

function robotsRow(check: SiteCheckDto): CheckRow {
  const { robots } = check
  if (!robots.startAllowed) {
    return { name: 'robots.txt', tone: 'err', text: 'robots.txt disallows the start page for SamoBot, so it is never requested.' }
  }
  const delay = `${robots.crawlDelaySeconds ?? 1} s between requests.`
  const listed = robots.disallowed.length === 0 ? '' : ` ${robots.disallowed.join(', ')} ${robots.disallowed.length === 1 ? 'is' : 'are'} disallowed.`
  return { name: 'robots.txt', tone: 'ok', text: `SamoBot may crawl this site. ${delay}${listed}` }
}

function sitemapRow(check: SiteCheckDto): CheckRow {
  const { sitemap } = check
  if (sitemap.state === 'Ready') {
    return { name: 'Sitemap', tone: 'ok', text: `${formatNumber(sitemap.files)} ${sitemap.files === 1 ? 'file' : 'files'} with ${formatNumber(sitemap.urls)} URLs.` }
  }
  if (sitemap.state === 'Pending') {
    return { name: 'Sitemap', tone: 'pending', text: 'Reading the sitemap. This can take a few minutes for a large site.' }
  }
  const why = sitemap.state === 'Failed' ? 'The sitemap could not be read.' : 'None found.'
  return { name: 'Sitemap', tone: 'warn', text: `${why} Pages are found by following links from the start page.` }
}

function browserRow(check: SiteCheckDto, needed: boolean): CheckRow {
  if (check.browser.available) {
    return { name: 'Browser service', tone: 'ok', text: 'Running. Pages can be rendered with JavaScript.' }
  }
  return { name: 'Browser service', tone: needed ? 'err' : 'warn', text: `Not available. ${check.browser.reason ?? 'The browser service did not answer.'}` }
}

function sizeOf(shell: { textLength: number; linkCount: number }): string {
  return `${formatNumber(shell.textLength)} characters and ${formatNumber(shell.linkCount)} ${shell.linkCount === 1 ? 'link' : 'links'}`
}

function scriptRow(check: SiteCheckDto): CheckRow {
  const { verdict, plain, rendered } = check.javaScript
  if (!check.startPage.reachable) {
    return { name: 'JavaScript', tone: 'warn', text: 'Not checked, because the start page could not be read.' }
  }
  if (verdict === 'NotNeeded') {
    return { name: 'JavaScript', tone: 'ok', text: `Not needed. The start page has ${sizeOf(plain)} without JavaScript.` }
  }
  if (verdict === 'Needed' && rendered) {
    return { name: 'JavaScript', tone: 'info', text: `Needed. Without JavaScript the start page has ${sizeOf(plain)}. In a browser it has ${sizeOf(rendered)}.` }
  }
  const shell = plain.emptyAppContainer ? ' It is an empty app shell.' : ''
  const why = check.browser.available ? 'The test render did not finish.' : 'Not confirmed: the browser service is not available.'
  return { name: 'JavaScript', tone: 'warn', text: `Probably needed. Without JavaScript the start page has ${sizeOf(plain)}.${shell} ${why}` }
}

export function needsScript(verdict: JsVerdict): boolean {
  return verdict !== 'NotNeeded'
}

export function checkRows(check: SiteCheckDto): CheckRow[] {
  return [startPageRow(check), robotsRow(check), sitemapRow(check), browserRow(check, needsScript(check.javaScript.verdict)), scriptRow(check)]
}

export function waitingRows(): CheckRow[] {
  return ['Start page', 'robots.txt', 'Sitemap', 'Browser service', 'JavaScript'].map((name) => ({ name, tone: 'pending', text: 'Checking…' }))
}
