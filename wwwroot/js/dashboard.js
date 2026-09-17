/**
 * Dashboard state, rendering and polling.
 *
 * Structure:
 *   api.js      - all HTTP calls
 *   charts.js   - all Chart.js configuration
 *   dashboard.js - state, DOM rendering, event handling (this file)
 *
 * Rows are built with DOM nodes and textContent rather than innerHTML, so data
 * from the database can never be interpreted as markup.
 */

import { api } from "./api.js";
import { renderSensorChart, renderProductionChart } from "./charts.js";

const REFRESH_MS = 15000;

const el = {
  factorySelect: document.getElementById("factorySelect"),
  refreshStatus: document.getElementById("refreshStatus"),
  errorBanner: document.getElementById("errorBanner"),
  fleetCards: document.getElementById("fleetCards"),
  factoryView: document.getElementById("factoryView"),
  factoryTitle: document.getElementById("factoryTitle"),
  factoryMeta: document.getElementById("factoryMeta"),
  emptyState: document.getElementById("emptyState"),
  kpiMachines: document.getElementById("kpiMachines"),
  kpiAlarms: document.getElementById("kpiAlarms"),
  kpiUnits: document.getElementById("kpiUnits"),
  kpiAvailability: document.getElementById("kpiAvailability"),
  alarmTable: document.getElementById("alarmTable"),
  sensorTable: document.getElementById("sensorTable"),
  alarmCount: document.getElementById("alarmCount"),
  sensorCount: document.getElementById("sensorCount"),
  sensorChart: document.getElementById("sensorChart"),
  productionChart: document.getElementById("productionChart"),
  toast: document.getElementById("toast"),
};

const state = {
  factories: [],
  selectedId: null,
};

/* ---------- small helpers ---------- */

const PILL_CLASS = {
  OK: "pill--ok",
  WARNING: "pill--warning",
  ALARM: "pill--critical",
  CRITICAL: "pill--critical",
};

const PILL_TEXT = {
  OK: "Normal",
  WARNING: "Warning",
  ALARM: "Alarm",
  CRITICAL: "Critical",
};

function pill(status) {
  const span = document.createElement("span");
  span.className = `pill ${PILL_CLASS[status] ?? "pill--warning"}`;
  span.textContent = PILL_TEXT[status] ?? status;
  return span;
}

function cell(text, className) {
  const td = document.createElement("td");
  td.textContent = text;
  if (className) td.className = className;
  return td;
}

function timeText(value) {
  if (!value) return "—";
  const date = new Date(value.endsWith?.("Z") ? value : `${value}Z`);
  return Number.isNaN(date.valueOf())
    ? "—"
    : date.toLocaleString("sv-SE", { dateStyle: "short", timeStyle: "medium" });
}

function emptyRow(tbody, columns, message) {
  const tr = document.createElement("tr");
  const td = document.createElement("td");
  td.colSpan = columns;
  td.className = "table__empty";
  td.textContent = message;
  tr.append(td);
  tbody.append(tr);
}

function showToast(message) {
  el.toast.textContent = message;
  el.toast.hidden = false;
  clearTimeout(showToast.timer);
  showToast.timer = setTimeout(() => { el.toast.hidden = true; }, 2500);
}

function showError(message) {
  el.errorBanner.textContent = message;
  el.errorBanner.hidden = false;
}

function clearError() {
  el.errorBanner.hidden = true;
}

function markRefreshed() {
  const now = new Date().toLocaleTimeString("sv-SE");
  el.refreshStatus.textContent = `Updated ${now}`;
}

/* ---------- fleet overview ---------- */

function renderFleet(overview) {
  el.fleetCards.replaceChildren();

  overview.forEach((factory) => {
    const card = document.createElement("button");
    card.type = "button";
    card.className = "fleet__card";
    card.dataset.factoryId = factory.id;
    card.setAttribute("aria-pressed", String(Number(factory.id) === Number(state.selectedId)));

    const name = document.createElement("p");
    name.className = "fleet__name";
    name.textContent = factory.name;

    const location = document.createElement("p");
    location.className = "fleet__location";
    location.textContent = factory.location;

    const stats = document.createElement("div");
    stats.className = "fleet__stats";
    stats.append(
      stat(factory.machine_count, "Machines"),
      stat(factory.active_alarms, "Alarms", Number(factory.active_alarms) > 0),
      stat(Number(factory.total_units ?? 0).toLocaleString("sv-SE"), "Units"),
    );

    card.append(name, location, stats);
    el.fleetCards.append(card);
  });
}

function stat(value, label, alert = false) {
  const wrapper = document.createElement("div");
  wrapper.className = "fleet__stat";

  const num = document.createElement("span");
  num.className = alert ? "fleet__num fleet__num--alert" : "fleet__num";
  num.textContent = value;

  const lbl = document.createElement("span");
  lbl.className = "fleet__lbl";
  lbl.textContent = label;

  wrapper.append(num, lbl);
  return wrapper;
}

/* ---------- selected factory ---------- */

function renderKpis({ sensors, alarms, production }) {
  const machines = new Set(sensors.map((s) => s.machine));
  el.kpiMachines.textContent = machines.size || "—";

  el.kpiAlarms.textContent = alarms.length;
  el.kpiAlarms.className = alarms.length > 0 ? "tile__value tile__value--alert" : "tile__value";

  const produced = production.reduce((sum, p) => sum + (p.units_produced ?? 0), 0);
  const rejected = production.reduce((sum, p) => sum + (p.units_rejected ?? 0), 0);
  el.kpiUnits.textContent = produced.toLocaleString("sv-SE");
  document.getElementById("kpiUnitsNote").textContent =
    produced > 0 ? `${((rejected / produced) * 100).toFixed(1)} % rejected` : "across all shifts";

  const runtime = production.reduce((sum, p) => sum + (p.runtime_min ?? 0), 0);
  const scheduled = runtime + production.reduce((sum, p) => sum + (p.downtime_min ?? 0), 0);
  el.kpiAvailability.textContent = scheduled > 0 ? `${Math.round((runtime / scheduled) * 100)} %` : "—";
}

function renderAlarms(alarms) {
  el.alarmTable.replaceChildren();
  el.alarmCount.textContent = alarms.length
    ? `${alarms.length} unacknowledged`
    : "";

  if (alarms.length === 0) {
    emptyRow(el.alarmTable, 6, "No active alarms.");
    return;
  }

  alarms.forEach((alarm) => {
    const tr = document.createElement("tr");

    const severity = document.createElement("td");
    severity.append(pill(alarm.severity));

    const action = document.createElement("td");
    const button = document.createElement("button");
    button.type = "button";
    button.className = "button";
    button.textContent = "Acknowledge";
    button.dataset.alarmId = alarm.id;
    action.append(button);

    tr.append(
      cell(alarm.machine, "machine"),
      cell(alarm.alarm_type),
      severity,
      cell(alarm.message, "message"),
      cell(timeText(alarm.created_at), "time"),
      action,
    );
    el.alarmTable.append(tr);
  });
}

function renderSensors(sensors) {
  el.sensorTable.replaceChildren();
  el.sensorCount.textContent = sensors.length ? `${sensors.length} readings` : "";

  if (sensors.length === 0) {
    emptyRow(el.sensorTable, 5, "No sensor data yet.");
    return;
  }

  sensors.forEach((reading) => {
    const tr = document.createElement("tr");

    const status = document.createElement("td");
    status.append(pill(reading.status));

    tr.append(
      cell(reading.machine, "machine"),
      cell(reading.sensor_type),
      cell(`${reading.value} ${reading.unit}`, "num"),
      status,
      cell(timeText(reading.recorded_at), "time"),
    );
    el.sensorTable.append(tr);
  });
}

async function loadFactory(id) {
  const [sensors, alarms, production] = await Promise.all([
    api.sensors(id),
    api.alarms(id),
    api.production(id),
  ]);

  renderKpis({ sensors, alarms, production });
  renderAlarms(alarms);
  renderSensors(sensors);
  renderSensorChart(el.sensorChart, sensors);
  renderProductionChart(el.productionChart, production);
  markRefreshed();
}

async function selectFactory(id) {
  if (!id) return;
  state.selectedId = Number(id);
  el.factorySelect.value = String(id);

  const factory = state.factories.find((f) => Number(f.id) === state.selectedId);
  el.factoryTitle.textContent = factory ? factory.name : "Factory";
  el.factoryMeta.textContent = factory ? `${factory.location}, ${factory.country}` : "";

  document.querySelectorAll(".fleet__card").forEach((card) => {
    card.setAttribute("aria-pressed", String(Number(card.dataset.factoryId) === state.selectedId));
  });

  el.emptyState.hidden = true;
  el.factoryView.hidden = false;

  try {
    await loadFactory(state.selectedId);
    clearError();
  } catch (error) {
    showError(`Could not load factory data: ${error.message}`);
  }
}

/* ---------- start-up, events, polling ---------- */

async function init() {
  try {
    const [factories, overview] = await Promise.all([api.factories(), api.overview()]);
    state.factories = factories;

    el.factorySelect.replaceChildren();
    const placeholder = document.createElement("option");
    placeholder.value = "";
    placeholder.textContent = "Select a factory";
    el.factorySelect.append(placeholder);

    factories.forEach((factory) => {
      const option = document.createElement("option");
      option.value = factory.id;
      option.textContent = `${factory.name} — ${factory.location}`;
      el.factorySelect.append(option);
    });

    renderFleet(overview);
    markRefreshed();
    clearError();
  } catch (error) {
    showError(`Could not reach the API: ${error.message}`);
    el.factorySelect.replaceChildren(new Option("Unavailable", ""));
  }
}

el.factorySelect.addEventListener("change", (event) => selectFactory(event.target.value));

el.fleetCards.addEventListener("click", (event) => {
  const card = event.target.closest(".fleet__card");
  if (card) selectFactory(card.dataset.factoryId);
});

el.alarmTable.addEventListener("click", async (event) => {
  const button = event.target.closest("button[data-alarm-id]");
  if (!button) return;

  button.disabled = true;
  button.textContent = "Saving…";

  try {
    await api.acknowledgeAlarm(button.dataset.alarmId);
    showToast("Alarm acknowledged");
    await refresh();
  } catch (error) {
    button.disabled = false;
    button.textContent = "Acknowledge";
    showError(`Could not acknowledge the alarm: ${error.message}`);
  }
});

async function refresh() {
  try {
    const overview = await api.overview();
    renderFleet(overview);
    if (state.selectedId) await loadFactory(state.selectedId);
    clearError();
  } catch (error) {
    showError(`Refresh failed: ${error.message}`);
  }
}

// Chart.js is loaded with defer, so wait for the window load event before drawing.
window.addEventListener("load", init);

// Poll while the tab is visible; stop when it is hidden to avoid pointless calls.
setInterval(() => {
  if (!document.hidden) refresh();
}, REFRESH_MS);

document.addEventListener("visibilitychange", () => {
  if (!document.hidden) refresh();
});
