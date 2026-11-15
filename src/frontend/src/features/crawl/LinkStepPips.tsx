import type { LinkSteps } from '@/types/api'
import { STEP_NAMES, stepLook } from './link-format'

/** Fetch, Store, Parse and Index of one link, each with a mark and a word for screen readers. */
export function LinkStepPips({ steps }: { steps: LinkSteps }) {
  return (
    <ol className="m-0 flex list-none flex-wrap gap-1 p-0">
      {STEP_NAMES.map(({ key, label }) => {
        const look = stepLook(steps[key])
        return (
          <li key={key} title={`${label}: ${look.word}`} className={`readout rounded-sm px-1.5 py-0.5 text-[11px] leading-4 ${look.className}`}>
            <span aria-hidden="true">{look.mark} </span>
            {label}
            <span className="sr-only">: {look.word}</span>
          </li>
        )
      })}
    </ol>
  )
}
