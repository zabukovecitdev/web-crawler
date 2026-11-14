import { useCallback, useEffect, useMemo, type ReactNode } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { setUnauthorizedHandler } from '@/lib/api'
import { AuthContext, type AuthState } from './auth-context'
import { fetchMe, meKey, signOutRequest } from './auth-api'

export function AuthProvider({ children }: { children: ReactNode }) {
  const client = useQueryClient()
  const query = useQuery({ queryKey: meKey, queryFn: fetchMe, retry: false, staleTime: 5 * 60_000 })

  useEffect(() => {
    // Any 401 from the API means the session is gone: drop the user, ProtectedRoute redirects.
    setUnauthorizedHandler(() => client.setQueryData(meKey, null))
    return () => setUnauthorizedHandler(null)
  }, [client])

  const refresh = useCallback(async () => {
    await client.invalidateQueries({ queryKey: meKey })
  }, [client])

  const signOut = useCallback(async () => {
    try {
      await signOutRequest()
    } finally {
      // Keep the /me query (it is observed); drop everything else cached for the old user.
      client.removeQueries({ predicate: (query) => query.queryKey[0] !== meKey[0] })
      client.setQueryData(meKey, null)
    }
  }, [client])

  const value = useMemo<AuthState>(
    () => ({ user: query.data ?? null, isLoading: query.isPending, refresh, signOut }),
    [query.data, query.isPending, refresh, signOut],
  )

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>
}
