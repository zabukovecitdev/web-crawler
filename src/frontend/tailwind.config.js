import animate from 'tailwindcss-animate'

// Every colour is a CSS variable holding space-separated RGB channels (see src/index.css),
// so opacity modifiers such as bg-primary/80 keep working.
const token = (name) => `rgb(var(--${name}) / <alpha-value>)`

const pair = (name) => ({ DEFAULT: token(name), foreground: token(`${name}-foreground`) })

/** @type {import('tailwindcss').Config} */
export default {
  darkMode: ['class'],
  content: ['./index.html', './src/**/*.{js,ts,jsx,tsx}'],
  theme: {
    extend: {
      fontFamily: {
        sans: ['Geist', 'Helvetica Neue', 'Arial', 'sans-serif'],
        mono: ['Geist Mono', 'SFMono-Regular', 'Menlo', 'Consolas', 'monospace'],
      },
      colors: {
        border: token('border'),
        input: token('input'),
        ring: token('ring'),
        background: token('background'),
        foreground: token('foreground'),
        primary: pair('primary'),
        secondary: pair('secondary'),
        destructive: pair('destructive'),
        muted: pair('muted'),
        accent: pair('accent'),
        popover: pair('popover'),
        card: pair('card'),
        graphite: { DEFAULT: token('graphite'), foreground: token('on-graphite') },
        'ink-faint': token('ink-faint'),
        'line-strong': token('line-strong'),
        signal: {
          100: token('signal-100'),
          200: token('signal-200'),
          300: token('signal-300'),
          400: token('signal-400'),
          500: token('primary'),
          ink: token('signal-ink'),
        },
        led: { DEFAULT: token('led'), soft: token('led-100'), ink: token('led-ink') },
        amber: { soft: token('amber-100'), ink: token('amber-ink') },
        chart: {
          1: token('chart-1'),
          2: token('chart-2'),
          3: token('chart-3'),
          4: token('chart-4'),
          5: token('chart-5'),
        },
        sidebar: {
          DEFAULT: token('sidebar-background'),
          foreground: token('sidebar-foreground'),
          primary: token('sidebar-primary'),
          'primary-foreground': token('sidebar-primary-foreground'),
          accent: token('sidebar-accent'),
          'accent-foreground': token('sidebar-accent-foreground'),
          border: token('sidebar-border'),
          ring: token('sidebar-ring'),
        },
      },
      borderRadius: {
        lg: 'var(--radius)',
        md: 'calc(var(--radius) - 2px)',
        sm: 'calc(var(--radius) - 4px)',
        xl: '0.875rem',
      },
      boxShadow: {
        'zb-1': 'var(--shadow-1)',
        'zb-2': 'var(--shadow-2)',
        'zb-inset': 'var(--shadow-inset)',
      },
    },
  },
  plugins: [animate],
}
