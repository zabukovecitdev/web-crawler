import { apiBase } from './config'

export class ApiError extends Error {
  readonly status: number
  readonly body: string

  constructor(status: number, message: string, body = '') {
    super(message)
    this.name = 'ApiError'
    this.status = status
    this.body = body
  }
}

export type ApiInit = RequestInit & { ignoreUnauthorized?: boolean }

let onUnauthorized: (() => void) | null = null

export function setUnauthorizedHandler(handler: (() => void) | null): void {
  onUnauthorized = handler
}

export function notifyUnauthorized(): void {
  onUnauthorized?.()
}

function isWrite(init?: RequestInit): boolean {
  const method = (init?.method ?? 'GET').toUpperCase()
  return method !== 'GET' && method !== 'HEAD'
}

function buildHeaders(init?: RequestInit): Headers {
  const headers = new Headers(init?.headers)
  if (!headers.has('Accept')) {
    headers.set('Accept', 'application/json')
  }
  if (isWrite(init)) {
    headers.set('X-Requested-With', 'samobot')
  }
  if (init?.body && !headers.has('Content-Type')) {
    headers.set('Content-Type', 'application/json')
  }
  return headers
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function validationMessages(errors: unknown): string | null {
  if (!isRecord(errors)) {
    return null
  }
  const parts = Object.values(errors)
    .flat()
    .filter((part): part is string => typeof part === 'string')
  return parts.length > 0 ? parts.join(' ') : null
}

function fromJson(value: unknown): string | null {
  if (typeof value === 'string') {
    return value
  }
  if (!isRecord(value)) {
    return null
  }
  const fields = [value.detail, value.title]
  const text = fields.find((field): field is string => typeof field === 'string' && field !== '')
  return validationMessages(value.errors) ?? text ?? null
}

export function readableError(text: string, fallback: string): string {
  if (text.trim() === '') {
    return fallback
  }
  try {
    return fromJson(JSON.parse(text)) ?? text
  } catch {
    return text
  }
}

export function errorCode(error: unknown): string | null {
  if (!(error instanceof ApiError)) {
    return null
  }
  try {
    const body: unknown = JSON.parse(error.body)
    return isRecord(body) && typeof body.code === 'string' ? body.code : null
  } catch {
    return null
  }
}

export function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : 'Something went wrong.'
}

async function toError(res: Response): Promise<ApiError> {
  const text = await res.text()
  return new ApiError(res.status, readableError(text, `${res.status} ${res.statusText}`), text)
}

export async function apiFetchResponse(path: string, init?: ApiInit): Promise<Response> {
  const { ignoreUnauthorized, ...rest } = init ?? {}
  const res = await fetch(`${apiBase}${path}`, {
    ...rest,
    credentials: 'include',
    headers: buildHeaders(rest),
  })
  if (res.status === 401 && !ignoreUnauthorized) {
    onUnauthorized?.()
  }
  if (!res.ok) {
    throw await toError(res)
  }
  return res
}

export async function apiFetch<T>(path: string, init?: ApiInit): Promise<T> {
  const res = await apiFetchResponse(path, init)
  if (res.status === 204) {
    return undefined as T
  }
  return res.json() as Promise<T>
}

type QueryValue = string | number | undefined | null

export function queryString(params: Record<string, QueryValue>): string {
  const search = new URLSearchParams()
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== null && value !== '') {
      search.set(key, String(value))
    }
  }
  const text = search.toString()
  return text ? `?${text}` : ''
}
