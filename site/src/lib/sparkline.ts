/**
 * Sparkline SVG Generator
 * Generates inline SVG sparklines for data visualization
 */

export interface SparklineOptions {
  width?: number;
  height?: number;
  strokeColor?: string;
  strokeWidth?: number;
  fillColor?: string;
  showDots?: boolean;
  dotRadius?: number;
  dotColor?: string;
}

const defaultOptions: Required<SparklineOptions> = {
  width: 60,
  height: 20,
  strokeColor: '#f97316', // accent orange
  strokeWidth: 1.5,
  fillColor: '',
  showDots: false,
  dotRadius: 2,
  dotColor: '#f97316',
};

/**
 * Generate an inline SVG sparkline from data points
 * @param data - Array of numeric values
 * @param options - Sparkline configuration options
 * @returns SVG string
 */
export function generateSparklineSVG(
  data: number[],
  options: SparklineOptions = {}
): string {
  const opts = { ...defaultOptions, ...options };
  const { width, height, strokeColor, strokeWidth, fillColor, showDots, dotRadius, dotColor } = opts;

  if (!data || data.length < 2) {
    // Return empty placeholder
    return `<svg width="${width}" height="${height}" class="sparkline"></svg>`;
  }

  const padding = 2;
  const chartWidth = width - padding * 2;
  const chartHeight = height - padding * 2;

  const min = Math.min(...data);
  const max = Math.max(...data);
  const range = max - min || 1; // Avoid division by zero

  // Calculate points
  const points = data.map((value, index) => {
    const x = padding + (index / (data.length - 1)) * chartWidth;
    const y = padding + chartHeight - ((value - min) / range) * chartHeight;
    return { x, y };
  });

  // Build path
  const pathD = points
    .map((p, i) => `${i === 0 ? 'M' : 'L'} ${p.x.toFixed(1)} ${p.y.toFixed(1)}`)
    .join(' ');

  // Build fill path (area under the line)
  let fillPath = '';
  if (fillColor) {
    fillPath = `
      <path
        d="${pathD} L ${points[points.length - 1].x.toFixed(1)} ${height - padding} L ${padding} ${height - padding} Z"
        fill="${fillColor}"
        opacity="0.3"
      />
    `;
  }

  // Build dots
  let dots = '';
  if (showDots) {
    dots = points
      .map((p, i) => {
        // Only show first and last dots, or all if less than 5 points
        if (data.length <= 5 || i === 0 || i === data.length - 1) {
          return `<circle cx="${p.x.toFixed(1)}" cy="${p.y.toFixed(1)}" r="${dotRadius}" fill="${dotColor}" />`;
        }
        return '';
      })
      .join('');
  }

  return `
    <svg width="${width}" height="${height}" class="sparkline" viewBox="0 0 ${width} ${height}">
      ${fillPath}
      <path
        d="${pathD}"
        fill="none"
        stroke="${strokeColor}"
        stroke-width="${strokeWidth}"
        stroke-linecap="round"
        stroke-linejoin="round"
      />
      ${dots}
    </svg>
  `.trim().replace(/\s+/g, ' ');
}

/**
 * Generate a sparkline with trend coloring (green for up, red for down)
 * @param data - Array of numeric values
 * @param options - Sparkline configuration options
 * @returns SVG string with appropriate trend color
 */
export function generateTrendSparkline(
  data: number[],
  options: SparklineOptions = {}
): string {
  if (!data || data.length < 2) {
    return generateSparklineSVG(data, options);
  }

  const trend = data[data.length - 1] - data[0];
  const trendColor = trend >= 0 ? '#22c55e' : '#ef4444'; // positive green, negative red

  return generateSparklineSVG(data, {
    ...options,
    strokeColor: options.strokeColor || trendColor,
    fillColor: options.fillColor || trendColor,
  });
}

/**
 * Generate sample data for testing/demo purposes
 * @param points - Number of data points
 * @param trend - 'up', 'down', or 'mixed'
 * @returns Array of sample values
 */
export function generateSampleData(
  points: number = 7,
  trend: 'up' | 'down' | 'mixed' = 'mixed'
): number[] {
  const data: number[] = [];
  let value = 50;

  for (let i = 0; i < points; i++) {
    const randomChange = (Math.random() - 0.5) * 20;
    let trendBias = 0;

    if (trend === 'up') {
      trendBias = 3;
    } else if (trend === 'down') {
      trendBias = -3;
    }

    value += randomChange + trendBias;
    value = Math.max(10, Math.min(90, value)); // Clamp between 10-90
    data.push(value);
  }

  return data;
}
