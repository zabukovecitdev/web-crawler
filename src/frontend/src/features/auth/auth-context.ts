import { createContext, useContext } from 'react'
import type { Me } from '@/types/api'

export type AuthState = {
  user: Me | null
  isLoading: boolean
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

export function useMe(): Me {
  const { user } = useAuth()
  if (!user) {
    throw new Error('useMe called without a signed-in user')
  }
  return user
}
