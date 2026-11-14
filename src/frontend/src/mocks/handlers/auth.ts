import { http, HttpResponse } from 'msw'
import type { Me } from '@/types/api'
import { json, noContent } from '../util'

export const MOCK_USER: Me = {
  userId: 'u-1',
  name: 'Rok Zabukovec',
  email: 'rok@example.com',
  pictureUrl: null,
}

// Mock session lives in memory: sign in once per page load, a reload keeps the cookie-less session.
let signedIn = true

export const authHandlers = [
  http.get('*/api/v1/me', () => (signedIn ? json(MOCK_USER) : new HttpResponse(null, { status: 401 }))),
  http.post('*/api/v1/auth/google', () => {
    signedIn = true
    return noContent()
  }),
  http.post('*/api/v1/auth/logout', () => {
    signedIn = false
    return noContent()
  }),
]
