import { afterEach, describe, expect, it, vi } from 'vitest'
import { ApiError, apiFetch, errorMessage, notifyUnauthorized, readableError, setUnauthorizedHandler } from './api'

describe('readableError', () => {
  it('joins the values of problem details errors', () => {
    const body = JSON.stringify({ title: 'Bad', errors: { Url: ['Url is required.'], Depth: ['Too deep.'] } })
    expect(readableError(body, 'fallback')).toBe('Url is required. Too deep.')
  })

  it('uses detail, then title', () => {
    expect(readableError(JSON.stringify({ title: 'Bad', detail: 'Nope.' }), 'f')).toBe('Nope.')
    expect(readableError(JSON.stringify({ title: 'Bad request' }), 'f')).toBe('Bad request')
  })

  it('unwraps a JSON string and keeps plain text', () => {
    expect(readableError('"Host is invalid."', 'f')).toBe('Host is invalid.')
    expect(readableError('Gateway timeout', 'f')).toBe('Gateway timeout')
  })

  it('falls back on an empty body', () => {
    expect(readableError('', '502 Bad Gateway')).toBe('502 Bad Gateway')
    expect(readableError('  ', '502 Bad Gateway')).toBe('502 Bad Gateway')
  })
})

describe('errorMessage', () => {
  it('reads Error messages and falls back otherwise', () => {
    expect(errorMessage(new ApiError(400, 'Bad', '{}'))).toBe('Bad')
    expect(errorMessage('x')).toBe('Something went wrong.')
  })
})

describe('apiFetch 401 handling', () => {
  afterEach(() => {
    vi.unstubAllGlobals()
    setUnauthorizedHandler(null)
  })

  it('calls the handler unless the request ignores 401', async () => {
    vi.stubGlobal('fetch', () => Promise.resolve(new Response('"no"', { status: 401 })))
    const handler = vi.fn()
    setUnauthorizedHandler(handler)
    await expect(apiFetch('/x')).rejects.toMatchObject({ status: 401, message: 'no' })
    expect(handler).toHaveBeenCalledTimes(1)
    await expect(apiFetch('/x', { ignoreUnauthorized: true })).rejects.toBeInstanceOf(ApiError)
    expect(handler).toHaveBeenCalledTimes(1)
    notifyUnauthorized()
    expect(handler).toHaveBeenCalledTimes(2)
  })
})
