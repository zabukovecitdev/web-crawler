import { analyticsHandlers } from './analytics'
import { authHandlers } from './auth'
import { crawlHandlers } from './crawl'
import { pagesHandlers } from './pages'
import { searchHandlers } from './search'
import { sitesHandlers } from './sites'

export const handlers = [
  ...authHandlers,
  ...sitesHandlers,
  ...pagesHandlers,
  ...searchHandlers,
  ...crawlHandlers,
  ...analyticsHandlers,
]
