import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import './index.css'
import App from './App.tsx'
import { useMocks } from '@/lib/config'

async function enableMocks(): Promise<void> {
  if (useMocks) {
    const { startMocks } = await import('@/mocks/browser')
    await startMocks()
  }
}

enableMocks().then(() => {
  createRoot(document.getElementById('root')!).render(
    <StrictMode>
      <App />
    </StrictMode>,
  )
})
