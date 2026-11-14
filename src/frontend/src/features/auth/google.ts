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
    script.onload = () => {
      const id = window.google?.accounts.id
      return id ? resolve(id) : reject(new Error('Google sign-in did not load'))
    }
    script.onerror = () => {
      pending = null
      reject(new Error('Could not reach accounts.google.com'))
    }
    document.head.appendChild(script)
  })
  return pending
}

export type { CredentialResponse }
