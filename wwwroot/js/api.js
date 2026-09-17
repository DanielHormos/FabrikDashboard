/**
 * Thin wrapper around the FabrikController REST API.
 * Every call goes through request(), so error handling and JSON parsing
 * live in one place instead of being repeated at each call site.
 */

const BASE = "/api/fabrik";

async function request(path, options = {}) {
  const response = await fetch(`${BASE}${path}`, options);

  if (!response.ok) {
    throw new Error(`${options.method ?? "GET"} ${path} failed: ${response.status}`);
  }

  // The acknowledge endpoint returns a small object; callers that ignore it are fine.
  return response.status === 204 ? null : response.json();
}

export const api = {
  factories: () => request("/factories"),
  overview: () => request("/overview"),
  sensors: (factoryId) => request(`/sensors?factoryId=${encodeURIComponent(factoryId)}`),
  alarms: (factoryId) => request(`/alarms?factoryId=${encodeURIComponent(factoryId)}`),
  production: (factoryId) => request(`/production?factoryId=${encodeURIComponent(factoryId)}`),
  acknowledgeAlarm: (alarmId) =>
    request(`/alarms/${encodeURIComponent(alarmId)}/acknowledge`, { method: "POST" }),
};
