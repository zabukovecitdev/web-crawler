import { apiFetch, ApiError } from '@/lib/api'
import type { GoogleSignInRequest, Me } from '@/types/api'

export const meKey = ['me'] as const

/** Returns null when nobody is signed in (401). */
export async function fetchMe(): Promise<Me | null> {
  try {
    return await apiFetch<Me>('/api/v1/me')
  } catch (error) {
    if (error instanceof ApiError && error.status === 401) {
      return null
    }
    throw error
  }
}

export function signInWithGoogle(credential: string): Promise<void> {
  const body: GoogleSignInRequest = { credential }
  return apiFetch<void>('/api/v1/auth/google', { method: 'POST', body: JSON.stringify(body) })
}

export function signOutRequest(): Promise<void> {
  return apiFetch<void>('/api/v1/auth/logout', { method: 'POST' })
}
