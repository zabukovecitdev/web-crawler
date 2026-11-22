export function validateUrl(value: string): string | null {
  const text = value.trim()
  if (!text) {
    return 'Enter the address of the site.'
  }
  try {
    const url = new URL(text)
    return url.protocol === 'http:' || url.protocol === 'https:' ? null : 'Use an address that starts with http:// or https://.'
  } catch {
    return 'This is not a full web address. Include https://.'
  }
}
