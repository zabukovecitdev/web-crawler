import { useEffect, useRef } from 'react'
import { Button } from '@/components/ui/button'
import { googleClientId, useMocks } from '@/lib/config'
import { initGoogle, loadGoogle, releaseGoogle } from './google'

type Props = {
  onCredential: (credential: string) => void
  onError: (message: string) => void
}

export function GoogleButton({ onCredential, onError }: Props) {
  const host = useRef<HTMLDivElement>(null)
  const credentialRef = useRef(onCredential)
  const errorRef = useRef(onError)

  useEffect(() => {
    credentialRef.current = onCredential
    errorRef.current = onError
  }, [onCredential, onError])

  useEffect(() => {
    if (!googleClientId || !host.current) {
      return
    }
    const parent = host.current
    let cancelled = false
    const handler = (credential: string) => credentialRef.current(credential)
    loadGoogle()
      .then((id) => {
        if (cancelled) {
          return
        }
        initGoogle(id, googleClientId, handler)
        parent.replaceChildren()
        id.renderButton(parent, { theme: 'outline', size: 'large', width: 380, text: 'continue_with' })
      })
      .catch((error: Error) => {
        if (!cancelled) {
          errorRef.current(error.message)
        }
      })
    return () => {
      cancelled = true
      releaseGoogle(handler)
      parent.replaceChildren()
    }
  }, [])

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
