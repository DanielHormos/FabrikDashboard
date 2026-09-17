-- ============================================================
-- Factory Operations Dashboard – databasschema för db-fabrikdata
-- Byggt från kolumnerna i FabrikController.cs, IoTHubTrigger.cs,
-- ArchiveFunction.cs och maskinnamnen i simulate_all.py.
-- Säkert att köra flera gånger: skapar bara det som saknas.
-- Kör i Azure Portal → db-fabrikdata → Query editor.
-- ============================================================

-- ---------- Tabeller ----------

IF OBJECT_ID('dbo.factories') IS NULL
CREATE TABLE dbo.factories (
    id        INT IDENTITY(1,1) PRIMARY KEY,
    name      NVARCHAR(100) NOT NULL UNIQUE,
    location  NVARCHAR(100) NOT NULL,
    country   NVARCHAR(100) NOT NULL,
    active    BIT NOT NULL DEFAULT 1
);

IF OBJECT_ID('dbo.machines') IS NULL
CREATE TABLE dbo.machines (
    id          INT IDENTITY(1,1) PRIMARY KEY,
    factory_id  INT NOT NULL REFERENCES dbo.factories(id),
    name        NVARCHAR(100) NOT NULL UNIQUE   -- Functionen slår upp maskin via namn
);

IF OBJECT_ID('dbo.sensor_data') IS NULL
CREATE TABLE dbo.sensor_data (
    id           BIGINT IDENTITY(1,1) PRIMARY KEY,
    factory_id   INT NOT NULL REFERENCES dbo.factories(id),
    machine_id   INT NOT NULL REFERENCES dbo.machines(id),
    sensor_type  NVARCHAR(50) NOT NULL,
    value        FLOAT NOT NULL,
    unit         NVARCHAR(20) NOT NULL,
    status       NVARCHAR(20) NOT NULL,        -- OK / WARNING / ALARM
    recorded_at  DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);

IF OBJECT_ID('dbo.alarms') IS NULL
CREATE TABLE dbo.alarms (
    id               INT IDENTITY(1,1) PRIMARY KEY,
    factory_id       INT NOT NULL REFERENCES dbo.factories(id),
    machine_id       INT NOT NULL REFERENCES dbo.machines(id),
    alarm_type       NVARCHAR(50) NOT NULL,
    severity         NVARCHAR(20) NOT NULL,    -- WARNING / CRITICAL
    message          NVARCHAR(500) NOT NULL,
    acknowledged     BIT NOT NULL DEFAULT 0,
    created_at       DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    acknowledged_at  DATETIME2 NULL,
    acknowledged_by  NVARCHAR(100) NULL
);

IF OBJECT_ID('dbo.production') IS NULL
CREATE TABLE dbo.production (
    id              INT IDENTITY(1,1) PRIMARY KEY,
    factory_id      INT NOT NULL REFERENCES dbo.factories(id),
    machine_id      INT NOT NULL REFERENCES dbo.machines(id),
    shift           NVARCHAR(20) NOT NULL,
    units_produced  INT NOT NULL DEFAULT 0,
    units_rejected  INT NOT NULL DEFAULT 0,
    runtime_min     INT NOT NULL DEFAULT 0,
    downtime_min    INT NOT NULL DEFAULT 0
);

-- ---------- Index för de frågor koden faktiskt kör ----------

-- "Senaste värdet per maskin+sensor" i /sensors, och 30-dagarsarkiveringen
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_sensor_data_machine_type_time')
CREATE INDEX IX_sensor_data_machine_type_time
    ON dbo.sensor_data (machine_id, sensor_type, recorded_at);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_sensor_data_recorded_at')
CREATE INDEX IX_sensor_data_recorded_at ON dbo.sensor_data (recorded_at);

-- Dedup-kontrollen i Functionen: finns redan ett okvitterat larm av samma typ?
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_alarms_machine_type_ack')
CREATE INDEX IX_alarms_machine_type_ack
    ON dbo.alarms (machine_id, alarm_type, acknowledged);

-- ---------- Grunddata ----------
-- Fabriks- och maskinnamnen MÅSTE matcha simulate_all.py, annars
-- hoppar Functionen tyst över meddelandet (machineId == 0).
-- Ort/land är platshållare – ändra till det du hade i rapporten.

IF NOT EXISTS (SELECT 1 FROM dbo.factories)
INSERT INTO dbo.factories (name, location, country) VALUES
    ('FabrikA', 'Stockholm', 'Sverige'),
    ('FabrikB', 'Göteborg',  'Sverige'),
    ('FabrikC', 'Malmö',     'Sverige');

IF NOT EXISTS (SELECT 1 FROM dbo.machines)
INSERT INTO dbo.machines (factory_id, name)
SELECT f.id, m.name
FROM (VALUES
    ('FabrikA', 'Press-1'), ('FabrikA', 'Welder-1'), ('FabrikA', 'Conveyor-1'), ('FabrikA', 'CNC-1'),
    ('FabrikB', 'Lathe-1'), ('FabrikB', 'Robot-1'),  ('FabrikB', 'Grinder-1'),
    ('FabrikC', 'Pump-1'),  ('FabrikC', 'Mixer-1'),  ('FabrikC', 'Scanner-1')
) AS m(factory, name)
JOIN dbo.factories f ON f.name = m.factory;

-- Produktionsdata genereras inte av simulatorn, så vi lägger in två skift per maskin
IF NOT EXISTS (SELECT 1 FROM dbo.production)
INSERT INTO dbo.production (factory_id, machine_id, shift, units_produced, units_rejected, runtime_min, downtime_min)
SELECT m.factory_id, m.id, s.shift,
       400 + ABS(CHECKSUM(NEWID())) % 400,   -- 400–799 enheter
       ABS(CHECKSUM(NEWID())) % 25,          -- 0–24 kasserade
       s.runtime,
       480 - s.runtime
FROM dbo.machines m
CROSS JOIN (VALUES ('Dag', 455), ('Kväll', 430)) AS s(shift, runtime);

-- ---------- Kontroll ----------
SELECT 'factories' AS tabell, COUNT(*) AS rader FROM dbo.factories
UNION ALL SELECT 'machines',    COUNT(*) FROM dbo.machines
UNION ALL SELECT 'production',  COUNT(*) FROM dbo.production
UNION ALL SELECT 'sensor_data', COUNT(*) FROM dbo.sensor_data
UNION ALL SELECT 'alarms',      COUNT(*) FROM dbo.alarms;
-- Förväntat: 3, 10, 20, 0, 0 (sensor_data och alarms fylls när simulatorn körs)
