import ReactMarkdown, { type Components } from 'react-markdown'
import remarkGfm from 'remark-gfm'
import { safeHref, safeImageSrc } from './markdown-urls'

const components: Components = {
  a({ href, children }) {
    const safe = safeHref(href)
    if (!safe) {
      return <span>{children}</span>
    }
    return (
      <a href={safe} target="_blank" rel="noopener noreferrer nofollow">
        {children}
      </a>
    )
  },
  img({ src, alt }) {
    const safe = typeof src === 'string' ? safeImageSrc(src) : undefined
    if (!safe) {
      return null
    }
    return <img src={safe} alt={alt ?? ''} loading="lazy" referrerPolicy="no-referrer" />
  },
}

/** Renders Markdown as React elements. Raw HTML is dropped, links and images are restricted to safe URLs. */
export function MarkdownRender({ markdown }: { markdown: string }) {
  return (
    <div className="markdown-body max-w-[72ch]">
      <ReactMarkdown remarkPlugins={[remarkGfm]} skipHtml components={components}>
        {markdown}
      </ReactMarkdown>
    </div>
  )
}
