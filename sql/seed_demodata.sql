-- ============================================================
-- Demodata för Factory Operations Dashboard
-- Fyller sensor_data och alarms med realistiska värden, med samma
-- intervall och gränsvärden som simulate_all.py använder.
--
-- Använd detta när Function-appen inte hinner fyllas i tid inför demon.
-- Kör i Azure Portal → db-fabrikdata → Query editor.
-- Kör schema.sql först (factories och machines måste finnas).
--
-- Rensa och fyll på nytt: sätt @rensa = 1.
-- ============================================================

DECLARE @rensa BIT = 1;            -- 1 = töm sensor_data och alarms först
DECLARE @matningar INT = 20;       -- antal mätvärden per sensor (20 = en timmes historik)

IF @rensa = 1
BEGIN
    DELETE FROM alarms;
    DELETE FROM sensor_data;
END

-- Sensordefinitioner, identiska med simulate_all.py
DECLARE @sensorer TABLE (
    machine     NVARCHAR(100),
    sensor_type NVARCHAR(50),
    minvarde    FLOAT,
    maxvarde    FLOAT,
    unit        NVARCHAR(20),
    grans       FLOAT
);

INSERT INTO @sensorer VALUES
    ('Press-1',    'temperature',  70,   95,  N'°C',   92),
    ('Press-1',    'pressure',    130,  160,  'bar',  158),
    ('Welder-1',   'temperature',  80,  100,  N'°C',   97),
    ('Welder-1',   'rpm',         900, 1400,  'RPM', 1380),
    ('Conveyor-1', 'rpm',         700, 1000,  'RPM',  980),
    ('CNC-1',      'vibration',     2,   10,  'mm/s',   8),
    ('CNC-1',      'temperature',  50,   80,  N'°C',   78),
    ('Lathe-1',    'temperature',  60,   90,  N'°C',   88),
    ('Lathe-1',    'rpm',        2800, 3500,  'RPM', 3450),
    ('Robot-1',    'temperature',  75,   95,  N'°C',   92),
    ('Robot-1',    'vibration',     1,    7,  'mm/s',   6),
    ('Grinder-1',  'pressure',     80,  110,  'bar',  108),
    ('Grinder-1',  'temperature',  45,   70,  N'°C',   68),
    ('Pump-1',     'pressure',    180,  220,  'bar',  215),
    ('Pump-1',     'temperature',  85,  110,  N'°C',  105),
    ('Mixer-1',    'rpm',         400,  600,  'RPM',  580),
    ('Mixer-1',    'vibration',     1,    5,  'mm/s',   4.5),
    ('Scanner-1',  'temperature',  30,   50,  N'°C',   48);

-- En rad per mätning: slumpat värde inom intervallet, status enligt gränsvärdet
;WITH tal AS (
    SELECT TOP (@matningar) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
    FROM sys.all_objects
),
matningar AS (
    SELECT
        m.factory_id,
        m.id AS machine_id,
        s.sensor_type,
        s.unit,
        s.grans,
        ROUND(s.minvarde + (ABS(CHECKSUM(NEWID())) % 1000) / 1000.0
              * (s.maxvarde - s.minvarde), 1) AS value,
        DATEADD(minute, -3 * (t.n - 1), SYSUTCDATETIME()) AS recorded_at
    FROM @sensorer s
    JOIN machines m ON m.name = s.machine
    CROSS JOIN tal t
)
INSERT INTO sensor_data (factory_id, machine_id, sensor_type, value, unit, status, recorded_at)
SELECT factory_id, machine_id, sensor_type, value, unit,
       CASE WHEN value >= grans          THEN 'ALARM'
            WHEN value >= grans * 0.92   THEN 'WARNING'
            ELSE 'OK' END,
       recorded_at
FROM matningar;

-- Larm skapas av det senaste värdet per maskin och sensortyp, och bara
-- ett aktivt larm per maskin och typ. Samma dedup-logik som i IoTHubTrigger.cs.
;WITH senaste AS (
    SELECT s.*,
           ROW_NUMBER() OVER (PARTITION BY s.machine_id, s.sensor_type
                              ORDER BY s.recorded_at DESC) AS rn
    FROM sensor_data s
)
INSERT INTO alarms (factory_id, machine_id, alarm_type, severity, message, created_at)
SELECT s.factory_id, s.machine_id, UPPER(s.sensor_type),
       CASE WHEN s.status = 'ALARM' THEN 'CRITICAL' ELSE 'WARNING' END,
       m.name + ' ' + s.sensor_type + ': '
           + CAST(s.value AS NVARCHAR(20)) + ' ' + s.unit,
       s.recorded_at
FROM senaste s
JOIN machines m ON m.id = s.machine_id
WHERE s.rn = 1 AND s.status <> 'OK';

-- ---------- Kontroll ----------
SELECT (SELECT COUNT(*) FROM sensor_data) AS sensorvarden,
       (SELECT COUNT(*) FROM alarms)      AS larm,
       (SELECT COUNT(*) FROM alarms WHERE severity = 'CRITICAL') AS kritiska;
