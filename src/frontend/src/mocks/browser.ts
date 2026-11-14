import { setupWorker } from 'msw/browser'
import { handlers } from './handlers'

export const worker = setupWorker(...handlers)

/** Starts MSW; unhandled requests (fonts, scripts) pass through. */
export async function startMocks(): Promise<void> {
  await worker.start({ onUnhandledRequest: 'bypass' })
}
