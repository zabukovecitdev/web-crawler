import { createContext, useContext } from 'react'
import type { Me } from '@/types/api'

export type AuthState = {
  /** The signed-in user, or null. */
  user: Me | null
  isLoading: boolean
  /** Re-reads /me, e.g. after the Google sign-in succeeded. */
  refresh: () => Promise<void>
  signOut: () => Promise<void>
}

export const AuthContext = createContext<AuthState | null>(null)

export function useAuth(): AuthState {
  const value = useContext(AuthContext)
  if (!value) {
    throw new Error('useAuth must be used inside <AuthProvider>')
  }
  return value
}

/** The signed-in user; only call below <ProtectedRoute>. */
export function useMe(): Me {
  const { user } = useAuth()
  if (!user) {
    throw new Error('useMe called without a signed-in user')
  }
  return user
}
