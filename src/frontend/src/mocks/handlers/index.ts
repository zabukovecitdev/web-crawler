import { analyticsHandlers } from './analytics'
import { authHandlers } from './auth'
import { crawlHandlers } from './crawl'
import { pagesHandlers } from './pages'
import { searchHandlers } from './search'
import { sitesHandlers } from './sites'

// Each feature agent edits only its own file in this folder.
export const handlers = [
  ...authHandlers,
  ...sitesHandlers,
  ...pagesHandlers,
  ...searchHandlers,
  ...crawlHandlers,
  ...analyticsHandlers,
]
