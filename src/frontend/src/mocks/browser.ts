import { setupWorker } from 'msw/browser'
import { handlers } from './handlers'
import { setBrowserUp } from './handlers/plan'

export const worker = setupWorker(...handlers)

export async function startMocks(): Promise<void> {
  await worker.start({ onUnhandledRequest: 'bypass' })
  Object.assign(window, { __browserMock: { set: setBrowserUp } })
}
