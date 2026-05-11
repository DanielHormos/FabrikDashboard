import json, time, random, threading, os
from azure.iot.device import IoTHubDeviceClient, Message

FACTORIES = {
    "FabrikA": {
        "conn": os.environ.get("IOTHUB_CONN_FABRIKA"),
        "machines": [
            {"machine": "Press-1", "sensors": [
                {"sensor_type": "temperature", "min": 70, "max": 95, "unit": "°C", "alarm_threshold": 92},
                {"sensor_type": "pressure", "min": 130, "max": 160, "unit": "bar", "alarm_threshold": 158},
            ]},
            {"machine": "Welder-1", "sensors": [
                {"sensor_type": "temperature", "min": 80, "max": 100, "unit": "°C", "alarm_threshold": 97},
                {"sensor_type": "rpm", "min": 900, "max": 1400, "unit": "RPM", "alarm_threshold": 1380},
            ]},
            {"machine": "Conveyor-1", "sensors": [
                {"sensor_type": "rpm", "min": 700, "max": 1000, "unit": "RPM", "alarm_threshold": 980},
            ]},
            {"machine": "CNC-1", "sensors": [
                {"sensor_type": "vibration", "min": 2, "max": 10, "unit": "mm/s", "alarm_threshold": 8},
                {"sensor_type": "temperature", "min": 50, "max": 80, "unit": "°C", "alarm_threshold": 78},
            ]},
        ]
    },
    "FabrikB": {
        "conn": os.environ.get("IOTHUB_CONN_FABRIKB"),
        "machines": [
            {"machine": "Lathe-1", "sensors": [
                {"sensor_type": "temperature", "min": 60, "max": 90, "unit": "°C", "alarm_threshold": 88},
                {"sensor_type": "rpm", "min": 2800, "max": 3500, "unit": "RPM", "alarm_threshold": 3450},
            ]},
            {"machine": "Robot-1", "sensors": [
                {"sensor_type": "temperature", "min": 75, "max": 95, "unit": "°C", "alarm_threshold": 92},
                {"sensor_type": "vibration", "min": 1, "max": 7, "unit": "mm/s", "alarm_threshold": 6},
            ]},
            {"machine": "Grinder-1", "sensors": [
                {"sensor_type": "pressure", "min": 80, "max": 110, "unit": "bar", "alarm_threshold": 108},
                {"sensor_type": "temperature", "min": 45, "max": 70, "unit": "°C", "alarm_threshold": 68},
            ]},
        ]
    },
    "FabrikC": {
        "conn": os.environ.get("IOTHUB_CONN_FABRIKC"),
        "machines": [
            {"machine": "Pump-1", "sensors": [
                {"sensor_type": "pressure", "min": 180, "max": 220, "unit": "bar", "alarm_threshold": 215},
                {"sensor_type": "temperature", "min": 85, "max": 110, "unit": "°C", "alarm_threshold": 105},
            ]},
            {"machine": "Mixer-1", "sensors": [
                {"sensor_type": "rpm", "min": 400, "max": 600, "unit": "RPM", "alarm_threshold": 580},
                {"sensor_type": "vibration", "min": 1, "max": 5, "unit": "mm/s", "alarm_threshold": 4.5},
            ]},
            {"machine": "Scanner-1", "sensors": [
                {"sensor_type": "temperature", "min": 30, "max": 50, "unit": "°C", "alarm_threshold": 48},
            ]},
        ]
    }
}

def get_status(value, threshold):
    if value >= threshold:
        return "ALARM"
    elif value >= threshold * 0.92:
        return "WARNING"
    return "OK"

def simulate_factory(factory_name, factory_data):
    if not factory_data["conn"]:
        print(f"❌ {factory_name}: connection string missing!")
        return
    client = IoTHubDeviceClient.create_from_connection_string(factory_data["conn"])
    client.connect()
    print(f"✅ {factory_name} connected!")
    try:
        while True:
            for machine in factory_data["machines"]:
                for sensor in machine["sensors"]:
                    value = round(random.uniform(sensor["min"], sensor["max"]), 1)
                    status = get_status(value, sensor["alarm_threshold"])
                    payload = {"machine": machine["machine"], "sensor_type": sensor["sensor_type"], "value": value, "unit": sensor["unit"], "status": status}
                    msg = Message(json.dumps(payload))
                    msg.content_encoding = "utf-8"
                    msg.content_type = "application/json"
                    client.send_message(msg)
                    print(f"📡 {factory_name} | {machine['machine']} | {sensor['sensor_type']}: {value} {sensor['unit']} [{status}]")
            time.sleep(10)
    except KeyboardInterrupt:
        client.disconnect()

threads = []
for name, data in FACTORIES.items():
    t = threading.Thread(target=simulate_factory, args=(name, data), daemon=True)
    threads.append(t)
    t.start()

print("\n🏭 All factories running! Press Ctrl+C to stop.\n")
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("\nStopping...")
