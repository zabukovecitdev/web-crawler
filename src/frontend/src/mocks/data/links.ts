import type { PageLinkDto } from '@/types/api'

export type LinkPeer = { id: number; url: string; code: number }

const ANCHORS = ['Read the guide', 'Configuration', 'Next step', null, 'See the reference', 'Back to start']
const TOTAL = 60

/** Edge cases the Links view has to handle: external, nofollow, never crawled, and a non-http URL. */
function specials(): PageLinkDto[] {
  return [
    { url: 'https://en.wikipedia.org/wiki/Web_crawler', text: 'Web crawler', internal: false, noFollow: false, pageId: null, statusCode: null },
    { url: 'https://tracker.example.net/out?id=42', text: 'Sponsored', internal: false, noFollow: true, pageId: null, statusCode: null },
    { url: 'https://docs.example.com/guide/draft-never-crawled', text: 'Draft guide', internal: true, noFollow: false, pageId: null, statusCode: null },
    { url: 'javascript:void(document.title)', text: 'Run now', internal: false, noFollow: false, pageId: null, statusCode: null },
  ]
}

/** Links on a page: the edge cases first, then internal links to the other crawled pages. */
export function outLinks(peers: LinkPeer[]): PageLinkDto[] {
  const links = specials()
  for (let i = 0; links.length < TOTAL && peers.length > 0; i++) {
    const peer = peers[i % peers.length]
    links.push({
      url: peer.url,
      text: ANCHORS[i % ANCHORS.length],
      internal: true,
      noFollow: i % 17 === 5,
      pageId: peer.id,
      statusCode: peer.code,
    })
  }
  return links
}

/** Pages that link to this page, with the anchor each used. */
export function inLinks(peers: LinkPeer[]): PageLinkDto[] {
  return peers.map((peer, i) => ({
    url: peer.url,
    text: ANCHORS[(i + 1) % ANCHORS.length],
    internal: true,
    noFollow: false,
    pageId: peer.id,
    statusCode: peer.code,
  }))
}
