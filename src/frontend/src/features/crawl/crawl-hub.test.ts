import { HttpError } from '@microsoft/signalr'
import { describe, expect, it } from 'vitest'
import { isUnauthorized } from './crawl-hub'

describe('isUnauthorized', () => {
  it('recognises an HttpError 401', () => {
    expect(isUnauthorized(new HttpError('Unauthorized', 401))).toBe(true)
  })

  it('recognises the wrapped negotiate failure', () => {
    const wrapped = new Error("Failed to complete negotiation with the server: Error: Unauthorized: Status code '401'")
    expect(isUnauthorized(wrapped)).toBe(true)
  })

  it('is false for a 500 and for other values', () => {
    expect(isUnauthorized(new HttpError('Server error', 500))).toBe(false)
    expect(isUnauthorized(new Error("Status code '500'"))).toBe(false)
    expect(isUnauthorized(undefined)).toBe(false)
  })
})
