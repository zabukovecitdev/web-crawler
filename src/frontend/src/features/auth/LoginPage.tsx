import { useCallback, useState } from 'react'
import { Navigate, useLocation, useNavigate } from 'react-router-dom'
import { Separator } from '@/components/ui/separator'
import { ApiError } from '@/lib/api'
import { useAuth } from './auth-context'
import { signInWithGoogle } from './auth-api'
import { GoogleButton } from './GoogleButton'

function messageFor(error: unknown): string {
  if (error instanceof ApiError && error.status === 403) {
    return 'This Google account is not on the approved list.'
  }
  return error instanceof Error ? error.message : 'Sign-in failed.'
}

function Hero() {
  return (
    <section className="flex flex-1 basis-[420px] flex-col justify-between gap-12 border-r border-border bg-graphite p-8 text-graphite-foreground md:p-12">
      <div className="flex items-center gap-2 text-lg font-semibold tracking-tight">
        <i aria-hidden="true" className="size-2.5 rounded-[2px] bg-primary" />
        SamoBot
      </div>
      <div className="flex max-w-[520px] flex-col gap-4">
        <h1 className="m-0 text-[36px] font-semibold leading-[40px] tracking-[-0.03em] md:text-5xl md:leading-[52px]">
          A polite web crawler, with everything it found.
        </h1>
        <div aria-hidden="true" className="h-px max-w-[320px] bg-primary" />
        <p className="opacity-80">Request a crawl, watch it run, then read and search every page it stored.</p>
      </div>
      <p className="label text-graphite-foreground opacity-75">User agent · SamoBot</p>
    </section>
  )
}

export function LoginPage() {
  const { user, refresh } = useAuth()
  const navigate = useNavigate()
  const location = useLocation()
  const [error, setError] = useState<string | null>(null)
  const from = (location.state as { from?: string } | null)?.from ?? '/'

  const onCredential = useCallback(
    (credential: string) => {
      setError(null)
      signInWithGoogle(credential)
        .then(refresh)
        .then(() => navigate(from, { replace: true }))
        .catch((e: unknown) => setError(messageFor(e)))
    },
    [refresh, navigate, from],
  )

  if (user) {
    return <Navigate to={from} replace />
  }

  return (
    <div className="flex min-h-screen flex-wrap items-stretch bg-background">
      <Hero />
      <main className="flex flex-1 basis-[420px] items-center justify-center px-6 py-12">
        <div className="flex w-full max-w-[380px] flex-col gap-6">
          <div className="flex flex-col gap-2">
            <p className="label">Dashboard</p>
            <h2 className="m-0 text-[32px] font-semibold leading-[38px] tracking-[-0.02em]">Sign in</h2>
            <p className="text-muted-foreground">Use the Google account that was approved for this dashboard.</p>
          </div>
          <GoogleButton onCredential={onCredential} onError={setError} />
          <Separator />
          <p className="text-[13px] leading-[18px] text-muted-foreground">
            Only approved accounts can sign in. If Google signs you in but the dashboard does not open, ask the
            owner to add your email address.
          </p>
          {error ? (
            <p className="text-[13px] leading-[18px] text-signal-ink" role="alert">
              <strong>Error:</strong> {error}
            </p>
          ) : null}
        </div>
      </main>
    </div>
  )
}
