import * as React from "react"
import * as RechartsPrimitive from "recharts"

import { cn } from "@/lib/utils"
import {
  getPayloadConfigFromPayload,
  useChart,
} from "@/components/ui/chart-context"

const ChartLegend = RechartsPrimitive.Legend

type LegendItem = NonNullable<RechartsPrimitive.LegendProps["payload"]>[number]

function LegendEntry({
  item,
  hideIcon,
  nameKey,
}: {
  item: LegendItem
  hideIcon: boolean
  nameKey?: string
}) {
  const { config } = useChart()
  const key = `${nameKey || item.dataKey || "value"}`
  const itemConfig = getPayloadConfigFromPayload(config, item, key)

  return (
    <div className="flex items-center gap-1.5 [&>svg]:h-3 [&>svg]:w-3 [&>svg]:text-muted-foreground">
      {itemConfig?.icon && !hideIcon ? (
        <itemConfig.icon />
      ) : (
        <div
          className="h-2 w-2 shrink-0 rounded-[2px]"
          style={{ backgroundColor: item.color }}
        />
      )}
      {itemConfig?.label}
    </div>
  )
}

const ChartLegendContent = React.forwardRef<
  HTMLDivElement,
  React.ComponentProps<"div"> &
    Pick<RechartsPrimitive.LegendProps, "payload" | "verticalAlign"> & {
      hideIcon?: boolean
      nameKey?: string
    }
>(
  (
    { className, hideIcon = false, payload, verticalAlign = "bottom", nameKey },
    ref
  ) => {
    if (!payload?.length) {
      return null
    }

    return (
      <div
        ref={ref}
        className={cn(
          "flex items-center justify-center gap-4",
          verticalAlign === "top" ? "pb-3" : "pt-3",
          className
        )}
      >
        {payload
          .filter((item) => item.type !== "none")
          .map((item) => (
            <LegendEntry
              key={item.value}
              item={item}
              hideIcon={hideIcon}
              nameKey={nameKey}
            />
          ))}
      </div>
    )
  }
)
ChartLegendContent.displayName = "ChartLegend"

export { ChartLegend, ChartLegendContent }
