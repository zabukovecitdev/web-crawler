import { cva } from "class-variance-authority"

export const badgeVariants = cva(
  "inline-flex items-center gap-1.5 rounded-sm border px-2 py-0.5 text-xs font-medium transition-colors focus:outline-none focus:ring-2 focus:ring-ring focus:ring-offset-2",
  {
    variants: {
      variant: {
        default:
          "border-transparent bg-primary text-primary-foreground shadow hover:bg-primary/80",
        secondary:
          "border-transparent bg-secondary text-secondary-foreground hover:bg-secondary/80",
        destructive:
          "border-transparent bg-destructive text-destructive-foreground shadow hover:bg-destructive/80",
        outline: "text-foreground",
        // Zabukovec tones: always paired with a word (and a dot where it means "live").
        success: "border-transparent bg-led-soft text-led-ink",
        warning: "border-transparent bg-amber-soft text-amber-ink",
        neutral: "border-transparent bg-muted text-foreground",
        signal: "border-transparent bg-signal-100 text-signal-ink",
      },
    },
    defaultVariants: {
      variant: "default",
    },
  }
)
