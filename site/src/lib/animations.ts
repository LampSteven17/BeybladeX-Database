/**
 * anime.js animation presets for BeybladeX Database.
 * Provides reusable animation functions for the cyberpunk theme.
 */

import anime from 'animejs';

/**
 * Stagger fade-in animation for lists/grids.
 * Elements fade in and slide up with a delay between each.
 */
export function staggerFadeIn(targets: string | HTMLElement | NodeListOf<Element> | HTMLCollection, options: { delay?: number; duration?: number; offset?: number } = {}) {
  // Convert HTMLCollection to array for anime.js compatibility
  const animationTargets = targets instanceof HTMLCollection ? Array.from(targets) : targets;
  const { delay = 50, duration = 400, offset = 20 } = options;

  return anime({
    targets: animationTargets,
    opacity: [0, 1],
    translateY: [offset, 0],
    duration,
    delay: anime.stagger(delay),
    easing: 'easeOutQuad',
  });
}

/**
 * Animate a number counter from 0 to target value.
 */
export function animateNumber(
  target: HTMLElement,
  endValue: number,
  options: { duration?: number; decimals?: number; prefix?: string; suffix?: string } = {}
) {
  const { duration = 1000, decimals = 0, prefix = '', suffix = '' } = options;

  const obj = { value: 0 };

  return anime({
    targets: obj,
    value: endValue,
    duration,
    easing: 'easeOutExpo',
    round: decimals === 0 ? 1 : Math.pow(10, decimals),
    update: () => {
      const formatted = decimals > 0 ? obj.value.toFixed(decimals) : Math.round(obj.value).toString();
      target.textContent = `${prefix}${formatted}${suffix}`;
    },
  });
}

/**
 * Animate a progress/stat bar width.
 */
export function animateBar(target: HTMLElement, percentage: number, options: { duration?: number; delay?: number } = {}) {
  const { duration = 800, delay = 0 } = options;

  return anime({
    targets: target,
    width: `${percentage}%`,
    duration,
    delay,
    easing: 'easeOutQuart',
  });
}

/**
 * Pulse glow effect for highlighting elements.
 */
export function pulseGlow(target: string | HTMLElement, options: { color?: string; intensity?: number; loop?: boolean } = {}) {
  const { color = '0, 240, 255', intensity = 0.5, loop = true } = options;

  return anime({
    targets: target,
    boxShadow: [`0 0 0 rgba(${color}, 0)`, `0 0 20px rgba(${color}, ${intensity})`, `0 0 0 rgba(${color}, 0)`],
    duration: 1500,
    easing: 'easeInOutSine',
    loop,
  });
}

/**
 * Slide in from side animation.
 */
export function slideIn(
  targets: string | HTMLElement | NodeListOf<Element>,
  options: { direction?: 'left' | 'right' | 'up' | 'down'; distance?: number; duration?: number; delay?: number } = {}
) {
  const { direction = 'left', distance = 50, duration = 500, delay = 0 } = options;

  const translateProp = direction === 'left' || direction === 'right' ? 'translateX' : 'translateY';
  const startValue = direction === 'left' || direction === 'up' ? -distance : distance;

  return anime({
    targets,
    [translateProp]: [startValue, 0],
    opacity: [0, 1],
    duration,
    delay,
    easing: 'easeOutCubic',
  });
}

/**
 * Ranking change animation (move up/down with fade).
 */
export function rankingChange(target: HTMLElement, direction: 'up' | 'down' | 'neutral', options: { duration?: number } = {}) {
  const { duration = 300 } = options;

  if (direction === 'neutral') return;

  const moveDistance = direction === 'up' ? -10 : 10;
  const color = direction === 'up' ? '#22c55e' : '#ef4444'; // green / red

  return anime
    .timeline({ targets: target })
    .add({
      translateY: moveDistance,
      backgroundColor: color,
      duration: duration / 2,
      easing: 'easeOutQuad',
    })
    .add({
      translateY: 0,
      backgroundColor: 'transparent',
      duration: duration / 2,
      easing: 'easeInQuad',
    });
}

/**
 * Card hover effect with scale and glow.
 */
export function cardHover(target: HTMLElement, enter: boolean) {
  return anime({
    targets: target,
    scale: enter ? 1.02 : 1,
    boxShadow: enter ? '0 0 30px rgba(0, 240, 255, 0.3)' : '0 0 0 rgba(0, 240, 255, 0)',
    duration: 200,
    easing: 'easeOutQuad',
  });
}

/**
 * Comparison bar animation (animate two bars from center).
 */
export function comparisonBars(
  bar1: HTMLElement,
  bar2: HTMLElement,
  value1: number,
  value2: number,
  options: { duration?: number; maxWidth?: number } = {}
) {
  const { duration = 1000, maxWidth = 100 } = options;

  const total = value1 + value2;
  const pct1 = total > 0 ? (value1 / total) * maxWidth : 50;
  const pct2 = total > 0 ? (value2 / total) * maxWidth : 50;

  return anime
    .timeline()
    .add(
      {
        targets: bar1,
        width: `${pct1}%`,
        duration,
        easing: 'easeOutQuart',
      },
      0
    )
    .add(
      {
        targets: bar2,
        width: `${pct2}%`,
        duration,
        easing: 'easeOutQuart',
      },
      0
    );
}

/**
 * Typewriter effect for text.
 */
export function typewriter(target: HTMLElement, text: string, options: { speed?: number; cursor?: boolean } = {}) {
  const { speed = 50, cursor = true } = options;

  target.textContent = '';
  if (cursor) {
    target.style.borderRight = '2px solid var(--accent-cyan)';
  }

  let index = 0;

  const interval = setInterval(() => {
    if (index < text.length) {
      target.textContent += text[index];
      index++;
    } else {
      clearInterval(interval);
      if (cursor) {
        // Blink cursor then remove
        setTimeout(() => {
          target.style.borderRight = 'none';
        }, 1000);
      }
    }
  }, speed);

  return {
    stop: () => clearInterval(interval),
  };
}

/**
 * Flash/highlight animation for updates.
 */
export function flashHighlight(target: HTMLElement, options: { color?: string; duration?: number } = {}) {
  const { color = 'rgba(0, 240, 255, 0.3)', duration = 500 } = options;

  return anime({
    targets: target,
    backgroundColor: [color, 'transparent'],
    duration,
    easing: 'easeOutQuad',
  });
}

/**
 * Initialize scroll-triggered animations.
 * Call this on page load to set up intersection observer for elements with data-animate attribute.
 */
export function initScrollAnimations() {
  if (typeof window === 'undefined') return;

  const observer = new IntersectionObserver(
    (entries) => {
      entries.forEach((entry) => {
        if (entry.isIntersecting) {
          const element = entry.target as HTMLElement;
          const animationType = element.dataset.animate;
          const delay = parseInt(element.dataset.animateDelay || '0', 10);

          setTimeout(() => {
            switch (animationType) {
              case 'fade-in':
                staggerFadeIn(element);
                break;
              case 'slide-left':
                slideIn(element, { direction: 'left' });
                break;
              case 'slide-right':
                slideIn(element, { direction: 'right' });
                break;
              case 'slide-up':
                slideIn(element, { direction: 'up' });
                break;
              default:
                staggerFadeIn(element);
            }
          }, delay);

          observer.unobserve(element);
        }
      });
    },
    { threshold: 0.1 }
  );

  document.querySelectorAll('[data-animate]').forEach((el) => {
    (el as HTMLElement).style.opacity = '0';
    observer.observe(el);
  });
}
