import { splitSnippet } from './snippet'

export function Snippet({ text }: { text: string }) {
  return (
    <p className="m-0 text-[15px] leading-[22px]">
      {splitSnippet(text).map((part, index) =>
        part.match ? <mark key={index}>{part.text}</mark> : <span key={index}>{part.text}</span>,
      )}
    </p>
  )
}
