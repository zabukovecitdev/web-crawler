import { useEffect, useRef } from 'react'
import { Button } from '@/components/ui/button'
import { googleClientId, useMocks } from '@/lib/config'
import { loadGoogle } from './google'

type Props = {
  onCredential: (credential: string) => void
  onError: (message: string) => void
}

export function GoogleButton({ onCredential, onError }: Props) {
  const host = useRef<HTMLDivElement>(null)

  useEffect(() => {
    if (!googleClientId || !host.current) {
      return
    }
    const parent = host.current
    loadGoogle()
      .then((id) => {
        id.initialize({ client_id: googleClientId, callback: (r) => onCredential(r.credential) })
        id.renderButton(parent, { theme: 'outline', size: 'large', width: 380, text: 'continue_with' })
      })
      .catch((error: Error) => onError(error.message))
  }, [onCredential, onError])

  if (!googleClientId && useMocks) {
    return (
      <Button size="lg" className="w-full" onClick={() => onCredential('mock-credential')}>
        Continue with Google
      </Button>
    )
  }
  if (!googleClientId) {
    return <p className="text-sm text-signal-ink">Error: VITE_GOOGLE_CLIENT_ID is not set.</p>
  }
  return <div ref={host} className="flex min-h-11 justify-center" />
}
