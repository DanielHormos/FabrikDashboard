/**
 * Chart rendering. Kept separate from the DOM/state code so the dashboard
 * logic stays readable and the chart styling lives in one place.
 *
 * Colour rules followed here:
 *   - Sensor bars use the reserved status colours (state, not identity), and the
 *     status is always also readable as text in the table and the legend.
 *   - Production uses two categorical colours (identity), with a legend.
 *   - One y-axis per chart, recessive grid, no gradients or shadows.
 */

const INK_SECONDARY = "#52514e";
const INK_MUTED = "#7c7b76";
const GRID = "#ebeae4";

const STATUS_COLOR = {
  OK: "#0ca30c",
  WARNING: "#fab219",
  ALARM: "#d03b3b",
};

const SERIES = { produced: "#2a78d6", rejected: "#eb6834" };

const charts = { sensor: null, production: null };

const baseOptions = {
  responsive: true,
  maintainAspectRatio: false,
  animation: { duration: 200 },
  plugins: {
    legend: { display: false }, // legends are rendered as HTML next to the titles
    tooltip: {
      backgroundColor: "#16161a",
      padding: 10,
      cornerRadius: 6,
      titleFont: { size: 12, weight: "600" },
      bodyFont: { size: 12 },
      displayColors: false,
    },
  },
  scales: {
    x: {
      grid: { display: false },
      border: { color: GRID },
      ticks: { color: INK_MUTED, font: { size: 10 }, maxRotation: 50, minRotation: 0 },
    },
    y: {
      beginAtZero: true,
      grid: { color: GRID, drawTicks: false },
      border: { display: false },
      ticks: { color: INK_MUTED, font: { size: 10 }, padding: 6 },
    },
  },
};

/** Deep-ish merge is overkill here; charts only override plugins and scales. */
function withOverrides(overrides = {}) {
  return {
    ...baseOptions,
    ...overrides,
    plugins: { ...baseOptions.plugins, ...(overrides.plugins ?? {}) },
    scales: {
      x: { ...baseOptions.scales.x, ...(overrides.scales?.x ?? {}) },
      y: { ...baseOptions.scales.y, ...(overrides.scales?.y ?? {}) },
    },
  };
}

/** True once the Chart.js CDN script has loaded. */
function chartLibraryReady() {
  return typeof Chart !== "undefined";
}

export function renderSensorChart(canvas, sensors) {
  if (!chartLibraryReady()) return; // tables still render without the chart library
  charts.sensor?.destroy();

  charts.sensor = new Chart(canvas, {
    type: "bar",
    data: {
      labels: sensors.map((s) => `${s.machine} · ${s.sensor_type}`),
      datasets: [
        {
          label: "Reading",
          data: sensors.map((s) => s.value),
          backgroundColor: sensors.map((s) => STATUS_COLOR[s.status] ?? INK_MUTED),
          borderRadius: 4,
          borderSkipped: false,
          maxBarThickness: 26,
        },
      ],
    },
    options: withOverrides({
      plugins: {
        tooltip: {
          callbacks: {
            label: (ctx) => {
              const s = sensors[ctx.dataIndex];
              return `${s.value} ${s.unit} — ${s.status}`;
            },
          },
        },
      },
    }),
  });
}

export function renderProductionChart(canvas, production) {
  if (!chartLibraryReady()) return;
  charts.production?.destroy();

  charts.production = new Chart(canvas, {
    type: "bar",
    data: {
      labels: production.map((p) => `${p.machine}${p.shift ? ` · ${p.shift}` : ""}`),
      datasets: [
        {
          label: "Produced",
          data: production.map((p) => p.units_produced),
          backgroundColor: SERIES.produced,
          borderRadius: 4,
          borderSkipped: false,
          maxBarThickness: 22,
        },
        {
          label: "Rejected",
          data: production.map((p) => p.units_rejected),
          backgroundColor: SERIES.rejected,
          borderRadius: 4,
          borderSkipped: false,
          maxBarThickness: 22,
        },
      ],
    },
    options: withOverrides({
      plugins: {
        tooltip: {
          displayColors: true,
          callbacks: {
            label: (ctx) => ` ${ctx.dataset.label}: ${ctx.parsed.y.toLocaleString()} units`,
          },
        },
      },
      scales: { x: { ticks: { color: INK_SECONDARY, font: { size: 10 } } } },
    }),
  });
}

export function destroyCharts() {
  charts.sensor?.destroy();
  charts.production?.destroy();
  charts.sensor = null;
  charts.production = null;
}
