"""
Genererar demodata direkt till Azure SQL: sensorvärden och larm.

Används när Function-appens Event Hub-lyssnare inte fyller databasen, så att
dashboarden kan demonstreras ändå. Sensorintervall och gränsvärden är
identiska med simulate_all.py, och larmlogiken är samma som i
IoTHubTrigger.cs: bara ett aktivt larm per maskin och sensortyp.

Körning (PowerShell):
    python -m pip install pymssql faker
    $env:SQL_PASSWORD='<lösenordet för sqladmin>'
    python .\seed_demodata.py --clear

Flaggor:
    --clear         töm sensor_data och alarms först
    --readings 20   antal mätvärden per sensor (20 x 3 min = en timmes historik)
    --interval 3    minuter mellan mätvärden
    --ack-one       kvittera ett av larmen, för att visa kvitteringsflödet
    --dry-run       visa vad som skulle skrivas, utan att skriva
"""

import argparse
import os
import random
import sys
from datetime import datetime, timedelta, timezone

SERVER = os.environ.get("SQL_SERVER", "sql-fabrikdata.database.windows.net")
DATABASE = os.environ.get("SQL_DB", "db-fabrikdata")
USER = os.environ.get("SQL_USER", "sqladmin")
PASSWORD = os.environ.get("SQL_PASSWORD")

# Samma definitioner som simulate_all.py: (maskin, sensortyp, min, max, enhet, gränsvärde)
SENSORS = [
    ("Press-1",    "temperature",   70,   95, "°C",   92),
    ("Press-1",    "pressure",     130,  160, "bar",  158),
    ("Welder-1",   "temperature",   80,  100, "°C",   97),
    ("Welder-1",   "rpm",          900, 1400, "RPM", 1380),
    ("Conveyor-1", "rpm",          700, 1000, "RPM",  980),
    ("CNC-1",      "vibration",      2,   10, "mm/s",   8),
    ("CNC-1",      "temperature",   50,   80, "°C",   78),
    ("Lathe-1",    "temperature",   60,   90, "°C",   88),
    ("Lathe-1",    "rpm",         2800, 3500, "RPM", 3450),
    ("Robot-1",    "temperature",   75,   95, "°C",   92),
    ("Robot-1",    "vibration",      1,    7, "mm/s",   6),
    ("Grinder-1",  "pressure",      80,  110, "bar",  108),
    ("Grinder-1",  "temperature",   45,   70, "°C",   68),
    ("Pump-1",     "pressure",     180,  220, "bar",  215),
    ("Pump-1",     "temperature",   85,  110, "°C",  105),
    ("Mixer-1",    "rpm",          400,  600, "RPM",  580),
    ("Mixer-1",    "vibration",      1,    5, "mm/s",  4.5),
    ("Scanner-1",  "temperature",   30,   50, "°C",   48),
]


def status_for(value, threshold):
    """Samma tröskellogik som simulatorn: 92 % av gränsen ger varning."""
    if value >= threshold:
        return "ALARM"
    if value >= threshold * 0.92:
        return "WARNING"
    return "OK"


def connect():
    """Ansluter med pymssql om det finns, annars pyodbc.

    Returnerar (anslutning, platshållare), eftersom de två drivrutinerna
    använder olika parametersyntax: %s respektive ?.
    """
    if not PASSWORD:
        sys.exit("SQL_PASSWORD är inte satt. Kör: $env:SQL_PASSWORD='<lösenord>'")

    try:
        import pymssql
    except ImportError:
        pymssql = None

    if pymssql is not None:
        return pymssql.connect(server=SERVER, user=USER, password=PASSWORD,
                               database=DATABASE), "%s"

    try:
        import pyodbc
    except ImportError:
        sys.exit("Ingen SQL-drivrutin hittades. Kör: python -m pip install pymssql")

    for driver in ("ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server"):
        if driver in pyodbc.drivers():
            return pyodbc.connect(
                f"DRIVER={{{driver}}};SERVER={SERVER};DATABASE={DATABASE};"
                f"UID={USER};PWD={PASSWORD};Encrypt=yes"
            ), "?"

    sys.exit("Ingen ODBC-drivrutin installerad. Kör: python -m pip install pymssql")


def load_machines(cur):
    """Maskinnamn till (machine_id, factory_id). Sensorvärden refererar båda."""
    cur.execute("SELECT id, factory_id, name FROM machines")
    return {row[2]: (row[0], row[1]) for row in cur.fetchall()}


def operator_name():
    """Svenskt namn till kvitteringen, om Faker finns installerat."""
    try:
        from faker import Faker
    except ImportError:
        return "Dashboard User"
    return Faker("sv_SE").name()


def generate(machines, readings_per_sensor, interval_minutes):
    """Skapar mätvärden bakåt i tiden, och larm från det senaste värdet."""
    now = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
    readings = []
    latest = {}

    for machine, sensor_type, low, high, unit, threshold in SENSORS:
        machine_id, factory_id = machines[machine]
        for n in range(readings_per_sensor):
            value = round(random.uniform(low, high), 1)
            state = status_for(value, threshold)
            recorded_at = now - timedelta(minutes=interval_minutes * n)
            readings.append((factory_id, machine_id, sensor_type, value,
                             unit, state, recorded_at))
            if n == 0:  # senaste värdet per maskin och sensor styr larmen
                latest[(machine, sensor_type)] = (factory_id, machine_id, value,
                                                  unit, state, recorded_at)

    alarms = []
    for (machine, sensor_type), values in latest.items():
        factory_id, machine_id, value, unit, state, recorded_at = values
        if state == "OK":
            continue
        alarms.append((
            factory_id, machine_id, sensor_type.upper(),
            "CRITICAL" if state == "ALARM" else "WARNING",
            f"{machine} {sensor_type}: {value} {unit}",
            recorded_at,
        ))

    return readings, alarms


def main():
    parser = argparse.ArgumentParser(description="Fyller db-fabrikdata med demodata.")
    parser.add_argument("--clear", action="store_true",
                        help="töm sensor_data och alarms först")
    parser.add_argument("--readings", type=int, default=20,
                        help="mätvärden per sensor (standard 20)")
    parser.add_argument("--interval", type=int, default=3,
                        help="minuter mellan mätvärden (standard 3)")
    parser.add_argument("--ack-one", action="store_true",
                        help="kvittera ett larm, för att visa kvitteringsflödet")
    parser.add_argument("--dry-run", action="store_true",
                        help="visa vad som skulle skrivas, utan att skriva")
    args = parser.parse_args()

    conn, ph = connect()
    cur = conn.cursor()

    machines = load_machines(cur)
    if not machines:
        sys.exit("Tabellen machines är tom. Kör sql/schema.sql först.")

    missing = {name for name, *_ in SENSORS} - machines.keys()
    if missing:
        sys.exit("Dessa maskiner saknas i databasen: " + ", ".join(sorted(missing)))

    readings, alarms = generate(machines, args.readings, args.interval)
    kritiska = sum(1 for a in alarms if a[3] == "CRITICAL")
    print(f"Genererat {len(readings)} sensorvärden och {len(alarms)} larm "
          f"({kritiska} kritiska).")

    if args.dry_run:
        for alarm in alarms:
            print(f"  {alarm[3]:8} {alarm[4]}")
        print("Dry run: ingenting skrevs till databasen.")
        conn.close()
        return

    if args.clear:
        cur.execute("DELETE FROM alarms")
        cur.execute("DELETE FROM sensor_data")
        print("Tömde sensor_data och alarms.")

    cur.executemany(
        "INSERT INTO sensor_data (factory_id, machine_id, sensor_type, value, unit,"
        f" status, recorded_at) VALUES ({', '.join([ph] * 7)})",
        readings,
    )
    if alarms:
        cur.executemany(
            "INSERT INTO alarms (factory_id, machine_id, alarm_type, severity, message,"
            f" created_at) VALUES ({', '.join([ph] * 6)})",
            alarms,
        )

    if args.ack_one and alarms:
        namn = operator_name()
        cur.execute(
            "UPDATE alarms SET acknowledged = 1, acknowledged_at = GETUTCDATE(),"
            f" acknowledged_by = {ph} WHERE id = (SELECT MIN(id) FROM alarms)",
            (namn,),
        )
        print(f"Kvitterade ett larm som {namn}.")

    conn.commit()

    cur.execute("SELECT (SELECT COUNT(*) FROM sensor_data), (SELECT COUNT(*) FROM alarms)")
    sensorvarden, larm = cur.fetchone()
    print(f"I databasen nu: {sensorvarden} sensorvärden, {larm} larm.")
    conn.close()


if __name__ == "__main__":
    main()
