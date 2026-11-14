import * as React from "react"
import * as RechartsPrimitive from "recharts"

import { cn } from "@/lib/utils"
import {
  type ChartConfig,
  getPayloadConfigFromPayload,
  useChart,
} from "@/components/ui/chart-context"

const ChartTooltip = RechartsPrimitive.Tooltip

type Indicator = "line" | "dot" | "dashed"
type TooltipProps = React.ComponentProps<typeof RechartsPrimitive.Tooltip> &
  React.ComponentProps<"div"> & {
    hideLabel?: boolean
    hideIndicator?: boolean
    indicator?: Indicator
    nameKey?: string
    labelKey?: string
  }
type Payload = NonNullable<TooltipProps["payload"]>
type PayloadItem = Payload[number]

function firstKey(...candidates: unknown[]) {
  return `${candidates.find(Boolean) ?? "value"}`
}

function labelValue(label: unknown, labelKey: string | undefined, itemLabel: React.ReactNode, config: ChartConfig) {
  if (labelKey || typeof label !== "string") {
    return itemLabel
  }
  return config[label]?.label ?? label
}

function resolveLabel(
  props: Pick<TooltipProps, "label" | "payload" | "labelKey" | "labelFormatter">,
  config: ChartConfig
) {
  const { label, payload = [], labelKey, labelFormatter } = props
  const [item] = payload
  const key = firstKey(labelKey, item?.dataKey, item?.name)
  const itemLabel = getPayloadConfigFromPayload(config, item, key)?.label
  const value = labelValue(label, labelKey, itemLabel, config)

  return labelFormatter ? labelFormatter(value, payload) : value
}

function Indicator({
  indicator,
  color,
  nested,
}: {
  indicator: Indicator
  color: string
  nested: boolean
}) {
  return (
    <div
      className={cn("shrink-0 rounded-[2px] border-[--color-border] bg-[--color-bg]", {
        "h-2.5 w-2.5": indicator === "dot",
        "w-1": indicator === "line",
        "w-0 border-[1.5px] border-dashed bg-transparent": indicator === "dashed",
        "my-0.5": nested && indicator === "dashed",
      })}
      style={{ "--color-bg": color, "--color-border": color } as React.CSSProperties}
    />
  )
}

type RowProps = {
  item: PayloadItem
  index: number
  config: ChartConfig
  indicator: Indicator
  hideIndicator: boolean
  nested: boolean
  label: React.ReactNode
  nameKey?: string
  color?: string
  formatter?: TooltipProps["formatter"]
}

function RowValue({ value }: { value: PayloadItem["value"] }) {
  if (!value) {
    return null
  }
  return (
    <span className="font-mono font-medium tabular-nums text-foreground">
      {value.toLocaleString()}
    </span>
  )
}

function RowMark(props: RowProps & { icon?: React.ComponentType }) {
  const { item, indicator, hideIndicator, nested, color, icon: Icon } = props
  if (Icon) {
    return <Icon />
  }
  if (hideIndicator) {
    return null
  }
  return (
    <Indicator
      indicator={indicator}
      color={color || item.payload.fill || item.color}
      nested={nested}
    />
  )
}

function RowBody(props: RowProps) {
  const { item, config, nested, label, nameKey } = props
  const key = firstKey(nameKey, item.name, item.dataKey)
  const itemConfig = getPayloadConfigFromPayload(config, item, key)

  return (
    <>
      <RowMark {...props} icon={itemConfig?.icon} />
      <div
        className={cn(
          "flex flex-1 justify-between leading-none",
          nested ? "items-end" : "items-center"
        )}
      >
        <div className="grid gap-1.5">
          {nested ? label : null}
          <span className="text-muted-foreground">{itemConfig?.label || item.name}</span>
        </div>
        <RowValue value={item.value} />
      </div>
    </>
  )
}

function TooltipRow(props: RowProps) {
  const { item, index, indicator, formatter } = props
  const { value, name } = item
  const custom = formatter && value !== undefined && name ? formatter(value, name, item, index, item.payload) : null

  return (
    <div
      className={cn(
        "flex w-full flex-wrap items-stretch gap-2 [&>svg]:h-2.5 [&>svg]:w-2.5 [&>svg]:text-muted-foreground",
        indicator === "dot" && "items-center"
      )}
    >
      {custom ?? <RowBody {...props} />}
    </div>
  )
}

function TooltipLabel({ props, config }: { props: TooltipProps; config: ChartConfig }) {
  const value = props.hideLabel ? null : resolveLabel(props, config)
  if (!value) {
    return null
  }
  return <div className={cn("font-medium", props.labelClassName)}>{value}</div>
}

const ChartTooltipContent = React.forwardRef<HTMLDivElement, TooltipProps>(
  (props, ref) => {
    const { active, payload, className, indicator = "dot" } = props
    const { config } = useChart()

    if (!active || !payload?.length) {
      return null
    }

    const label = <TooltipLabel props={props} config={config} />
    const nested = payload.length === 1 && indicator !== "dot"

    return (
      <div
        ref={ref}
        className={cn(
          "grid min-w-[8rem] items-start gap-1.5 rounded-lg border border-border/50 bg-background px-2.5 py-1.5 text-xs shadow-xl",
          className
        )}
      >
        {!nested ? label : null}
        <div className="grid gap-1.5">
          {payload
            .filter((item) => item.type !== "none")
            .map((item, index) => (
              <TooltipRow
                key={item.dataKey}
                item={item}
                index={index}
                config={config}
                indicator={indicator}
                hideIndicator={props.hideIndicator ?? false}
                nested={nested}
                label={label}
                nameKey={props.nameKey}
                color={props.color}
                formatter={props.formatter}
              />
            ))}
        </div>
      </div>
    )
  }
)
ChartTooltipContent.displayName = "ChartTooltip"

export { ChartTooltip, ChartTooltipContent }
