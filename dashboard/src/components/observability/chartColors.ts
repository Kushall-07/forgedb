/**
 * Recharts renders stroke/fill as raw SVG presentation attributes, which do
 * not reliably resolve `var(--token)` across browsers the way a CSS property
 * does. These mirror the status tokens in src/styles/tokens.css literally, so
 * charts stay on the same palette as the rest of the console without
 * depending on CSS variable resolution inside SVG attributes.
 */
export const CHART_COLORS = {
  sync: '#35d8e0',
  identity: '#9c8cff',
  warn: '#f0ab3d',
  critical: '#f0555c',
  inkSoft: '#8d93a3',
  hairline: 'rgba(241, 242, 245, 0.12)',
} as const;
