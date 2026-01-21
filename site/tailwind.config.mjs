/** @type {import('tailwindcss').Config} */
export default {
  content: ['./src/**/*.{astro,html,js,jsx,md,mdx,svelte,ts,tsx,vue}'],
  theme: {
    extend: {
      colors: {
        'cyber-bg': '#0a0a0f',
        'cyber-card': '#12121a',
        'cyber-card-light': '#1a1a25',
        'cyber-cyan': '#00f0ff',
        'cyber-pink': '#ff00aa',
        'cyber-purple': '#8b5cf6',
        'cyber-text': '#e4e4e7',
        'cyber-muted': '#71717a',
        'cyber-border': '#2a2a35',
      },
      boxShadow: {
        'glow-cyan': '0 0 20px rgba(0, 240, 255, 0.3)',
        'glow-pink': '0 0 20px rgba(255, 0, 170, 0.3)',
        'glow-purple': '0 0 20px rgba(139, 92, 246, 0.3)',
        'glow-cyan-sm': '0 0 10px rgba(0, 240, 255, 0.2)',
        'glow-pink-sm': '0 0 10px rgba(255, 0, 170, 0.2)',
      },
      fontFamily: {
        mono: ['JetBrains Mono', 'Fira Code', 'Consolas', 'Monaco', 'monospace'],
        sans: ['Inter', 'system-ui', 'sans-serif'],
      },
      animation: {
        'pulse-glow': 'pulse-glow 2s ease-in-out infinite',
        'scan': 'scan 4s linear infinite',
        'flicker': 'flicker 0.15s infinite',
      },
      keyframes: {
        'pulse-glow': {
          '0%, 100%': { opacity: '1' },
          '50%': { opacity: '0.7' },
        },
        scan: {
          '0%': { transform: 'translateY(-100%)' },
          '100%': { transform: 'translateY(100%)' },
        },
        flicker: {
          '0%, 100%': { opacity: '1' },
          '50%': { opacity: '0.8' },
        },
      },
      backgroundImage: {
        'gradient-radial': 'radial-gradient(var(--tw-gradient-stops))',
        'cyber-gradient': 'linear-gradient(135deg, #00f0ff 0%, #8b5cf6 50%, #ff00aa 100%)',
      },
    },
  },
  plugins: [],
};
