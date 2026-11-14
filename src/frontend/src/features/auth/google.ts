// Minimal types for Google Identity Services (https://accounts.google.com/gsi/client).
type CredentialResponse = { credential: string }

type GoogleId = {
  initialize: (config: { client_id: string; callback: (response: CredentialResponse) => void }) => void
  renderButton: (parent: HTMLElement, options: Record<string, string | number>) => void
}

declare global {
  interface Window {
    google?: { accounts: { id: GoogleId } }
  }
}

const SRC = 'https://accounts.google.com/gsi/client'

let pending: Promise<GoogleId> | null = null

/** Loads the GIS script once and resolves with its `accounts.id` API. */
export function loadGoogle(): Promise<GoogleId> {
  pending ??= new Promise<GoogleId>((resolve, reject) => {
    const script = document.createElement('script')
    script.src = SRC
    script.async = true
    const fail = (message: string) => {
      pending = null
      script.remove()
      reject(new Error(message))
    }
    script.onload = () => {
      const id = window.google?.accounts.id
      return id ? resolve(id) : fail('Google sign-in did not load')
    }
    script.onerror = () => fail('Could not reach accounts.google.com')
    document.head.appendChild(script)
  })
  return pending
}

let initializedFor: string | null = null
let currentCallback: ((credential: string) => void) | null = null

/** Initializes GIS once per client id; the latest callback receives the credential. */
export function initGoogle(id: GoogleId, clientId: string, callback: (credential: string) => void): void {
  currentCallback = callback
  if (initializedFor === clientId) {
    return
  }
  id.initialize({ client_id: clientId, callback: (r) => currentCallback?.(r.credential) })
  initializedFor = clientId
}

/** Stops delivering credentials to this callback (the button unmounted). */
export function releaseGoogle(callback: (credential: string) => void): void {
  if (currentCallback === callback) {
    currentCallback = null
  }
}

export type { CredentialResponse }
