import { analyticsHandlers } from './analytics'
import { authHandlers } from './auth'
import { crawlHandlers } from './crawl'
import { pagesHandlers } from './pages'
import { planHandlers } from './plan'
import { searchHandlers } from './search'
import { sitesHandlers } from './sites'

export const handlers = [
  ...authHandlers,
  ...sitesHandlers,
  ...pagesHandlers,
  ...searchHandlers,
  ...crawlHandlers,
  ...planHandlers,
  ...analyticsHandlers,
]
