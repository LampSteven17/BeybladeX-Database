/** @type {import('tailwindcss').Config} */
export default {
  content: ['./src/**/*.{astro,html,js,jsx,md,mdx,svelte,ts,tsx,vue}'],
  theme: {
    extend: {
      colors: {
        // ===========================================
        // Terminal Background hierarchy (Bloomberg style)
        // ===========================================
        'terminal-bg': '#0d1117',       // Main background
        'terminal-surface': '#161b22',  // Cards, panels
        'terminal-elevated': '#21262d', // Elevated elements
        'terminal-border': '#30363d',   // Borders

        // Legacy aliases (for backwards compatibility during transition)
        'canvas': '#0d1117',
        'surface': '#161b22',
        'panel': '#21262d',
        'hover': '#30363d',

        // ===========================================
        // Text hierarchy
        // ===========================================
        'fg': '#f5f5f5',                // Main text, headings
        'fg-secondary': '#a3a3a3',      // Body text, descriptions
        'fg-muted': '#666666',          // Subtle text, labels
        'fg-disabled': '#404040',       // Disabled states

        // ===========================================
        // Border colors
        // ===========================================
        'stroke': '#30363d',            // Main borders (terminal style)
        'stroke-subtle': '#21262d',     // Subtle dividers
        'stroke-hover': '#484f58',      // Hover state borders

        // ===========================================
        // Accent colors - Primary (Orange)
        // ===========================================
        'accent': '#f97316',            // Primary accent (orange-500)
        'accent-light': '#fb923c',      // Lighter accent
        'accent-dark': '#c2410c',       // Darker accent
        'accent-muted': '#431407',      // Very subtle orange bg

        // ===========================================
        // Gold - Champion/Hero color
        // ===========================================
        'gold': '#fbbf24',              // Gold for champion spotlight

        // ===========================================
        // Series colors (matching official packaging)
        // ===========================================
        'series-bx': '#0068B7',         // Blue - Basic series (official packaging blue)
        'series-bx-muted': '#0a2540',   // Blue muted bg
        'series-ux': '#F26522',         // Orange - Unique series (official packaging orange)
        'series-ux-muted': '#3d1a0a',   // Orange muted bg
        'series-cx': '#E4007F',         // Pink/Magenta - Custom series (official packaging pink)
        'series-cx-muted': '#3d0a2a',   // Pink muted bg

        // ===========================================
        // Status colors (gains/losses, trends)
        // ===========================================
        'positive': '#22c55e',          // Green - gains, rising
        'positive-muted': '#14532d',    // Green muted bg
        'negative': '#ef4444',          // Red - losses, falling
        'negative-muted': '#450a0a',    // Red muted bg

        // ===========================================
        // Ratchet height colors (spectrum: green -> yellow -> pink -> red)
        // ===========================================
        'ratchet-50': '#22C55E',         // Green (lowest)
        'ratchet-50-muted': '#14532d',
        'ratchet-55': '#84CC16',         // Lime / Yellow-green
        'ratchet-55-muted': '#365314',
        'ratchet-60': '#FBBF24',         // Yellow / Amber
        'ratchet-60-muted': '#451a03',
        'ratchet-65': '#F97316',         // Orange
        'ratchet-65-muted': '#431407',
        'ratchet-70': '#EC4899',         // Pink
        'ratchet-70-muted': '#500724',
        'ratchet-80': '#F43F5E',         // Rose / Hot pink
        'ratchet-80-muted': '#4c0519',
        'ratchet-85': '#DC2626',         // Red (highest)
        'ratchet-85-muted': '#450a0a',

        // ===========================================
        // Tier/Rank colors (financial grade style)
        // ===========================================
        'tier-ss': '#fbbf24',           // Gold - SS tier (elite)
        'tier-s': '#f59e0b',            // Amber - S tier
        'tier-a': '#f97316',            // Orange - A tier
        'tier-b': '#3b82f6',            // Blue - B tier
        'tier-c': '#6b7280',            // Gray - C tier
        'tier-d': '#4b5563',            // Dark gray - D tier
        'tier-f': '#374151',            // Darker gray - F tier
      },
      boxShadow: {
        'sm': '0 1px 2px rgba(0, 0, 0, 0.4)',
        'card': '0 1px 3px rgba(0, 0, 0, 0.3)',
        'card-hover': '0 4px 12px rgba(0, 0, 0, 0.5)',
        'glow-accent': '0 0 20px rgba(249, 115, 22, 0.15)',
        'glow-gold': '0 0 30px rgba(251, 191, 36, 0.3)',
        'glow-gold-lg': '0 0 60px rgba(251, 191, 36, 0.2)',
      },
      fontFamily: {
        mono: ['JetBrains Mono', 'SF Mono', 'Consolas', 'monospace'],
        sans: ['Inter', '-apple-system', 'BlinkMacSystemFont', 'system-ui', 'sans-serif'],
      },
      animation: {
        'fade-in': 'fade-in 0.15s ease-out',
        'slide-up': 'slide-up 0.2s ease-out',
        'ticker': 'ticker 30s linear infinite',
        'glow-pulse': 'glow-pulse 2s ease-in-out infinite',
        'border-glow': 'border-glow 2s ease-in-out infinite',
      },
      keyframes: {
        'fade-in': {
          '0%': { opacity: '0' },
          '100%': { opacity: '1' },
        },
        'slide-up': {
          '0%': { opacity: '0', transform: 'translateY(4px)' },
          '100%': { opacity: '1', transform: 'translateY(0)' },
        },
        'ticker': {
          '0%': { transform: 'translateX(0)' },
          '100%': { transform: 'translateX(-50%)' },
        },
        'glow-pulse': {
          '0%, 100%': { opacity: '0.4' },
          '50%': { opacity: '0.8' },
        },
        'border-glow': {
          '0%, 100%': { 'box-shadow': '0 0 10px rgba(251, 191, 36, 0.3)' },
          '50%': { 'box-shadow': '0 0 20px rgba(251, 191, 36, 0.5)' },
        },
      },
      spacing: {
        '18': '4.5rem',
        '22': '5.5rem',
      },
      maxWidth: {
        '8xl': '88rem',  // 1408px - wider container
        '9xl': '100rem', // 1600px - widest container
      },
    },
  },
  plugins: [],
};
