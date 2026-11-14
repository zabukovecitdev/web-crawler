import * as React from "react"
import * as RechartsPrimitive from "recharts"

import { cn } from "@/lib/utils"
import {
  type ChartConfig,
  ChartContext,
  THEMES,
} from "@/components/ui/chart-context"

export type { ChartConfig }

function themeCss(id: string, config: ChartConfig, theme: keyof typeof THEMES) {
  const lines = Object.entries(config)
    .map(([key, item]) => {
      const color = item.theme?.[theme] || item.color
      return color ? `  --color-${key}: ${color};` : null
    })
    .join("\n")

  return `\n${THEMES[theme]} [data-chart=${id}] {\n${lines}\n}\n`
}

const ChartStyle = ({ id, config }: { id: string; config: ChartConfig }) => {
  const colorConfig = Object.values(config).filter((item) => item.theme || item.color)

  if (!colorConfig.length) {
    return null
  }

  const css = (Object.keys(THEMES) as (keyof typeof THEMES)[])
    .map((theme) => themeCss(id, config, theme))
    .join("\n")

  return <style dangerouslySetInnerHTML={{ __html: css }} />
}

const ChartContainer = React.forwardRef<
  HTMLDivElement,
  React.ComponentProps<"div"> & {
    config: ChartConfig
    children: React.ComponentProps<
      typeof RechartsPrimitive.ResponsiveContainer
    >["children"]
  }
>(({ id, className, children, config, ...props }, ref) => {
  const uniqueId = React.useId()
  const chartId = `chart-${id || uniqueId.replace(/:/g, "")}`

  return (
    <ChartContext.Provider value={{ config }}>
      <div
        data-chart={chartId}
        ref={ref}
        className={cn(
          "flex aspect-video justify-center text-xs [&_.recharts-cartesian-axis-tick_text]:fill-muted-foreground [&_.recharts-cartesian-grid_line[stroke='#ccc']]:stroke-border/50 [&_.recharts-curve.recharts-tooltip-cursor]:stroke-border [&_.recharts-dot[stroke='#fff']]:stroke-transparent [&_.recharts-layer]:outline-none [&_.recharts-polar-grid_[stroke='#ccc']]:stroke-border [&_.recharts-radial-bar-background-sector]:fill-muted [&_.recharts-rectangle.recharts-tooltip-cursor]:fill-muted [&_.recharts-reference-line_[stroke='#ccc']]:stroke-border [&_.recharts-sector[stroke='#fff']]:stroke-transparent [&_.recharts-sector]:outline-none [&_.recharts-surface]:outline-none",
          className
        )}
        {...props}
      >
        <ChartStyle id={chartId} config={config} />
        <RechartsPrimitive.ResponsiveContainer>
          {children}
        </RechartsPrimitive.ResponsiveContainer>
      </div>
    </ChartContext.Provider>
  )
})
ChartContainer.displayName = "Chart"

export { ChartContainer, ChartStyle }
export { ChartTooltip, ChartTooltipContent } from "@/components/ui/chart-tooltip"
export { ChartLegend, ChartLegendContent } from "@/components/ui/chart-legend"
