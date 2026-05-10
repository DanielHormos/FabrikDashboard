using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Microsoft.Data.SqlClient;
using System.Text.Json;

public class IoTHubTrigger
{
    private readonly ILogger _logger;
    private readonly string _conn = "Server=sql-fabrikdata.database.windows.net;Database=db-fabrikdata;User Id=sqladmin;Password=Fabrik123!;Encrypt=True;";

    public IoTHubTrigger(ILoggerFactory loggerFactory)
    {
        _logger = loggerFactory.CreateLogger<IoTHubTrigger>();
    }

    [Function("IoTHubTrigger")]
    public async Task Run(
        [EventHubTrigger("messages/events", Connection = "IoTHubConnection", ConsumerGroup = "functions")] string[] messages)
    {
        foreach (var message in messages)
        {
            try
            {
                var data = JsonSerializer.Deserialize<SensorMessage>(message);
                if (data == null) continue;

                using var conn = new SqlConnection(_conn);
                await conn.OpenAsync();

                var lookupCmd = new SqlCommand("SELECT m.id, m.factory_id FROM machines m WHERE m.name = @machine", conn);
                lookupCmd.Parameters.AddWithValue("@machine", data.machine);

                int machineId = 0, factoryId = 0;
                using (var reader = await lookupCmd.ExecuteReaderAsync())
                {
                    if (await reader.ReadAsync())
                    {
                        machineId = reader.GetInt32(0);
                        factoryId = reader.GetInt32(1);
                    }
                }

                if (machineId == 0) continue;

                var insertCmd = new SqlCommand("INSERT INTO sensor_data (factory_id, machine_id, sensor_type, value, unit, status) VALUES (@fid, @mid, @type, @val, @unit, @status)", conn);
                insertCmd.Parameters.AddWithValue("@fid", factoryId);
                insertCmd.Parameters.AddWithValue("@mid", machineId);
                insertCmd.Parameters.AddWithValue("@type", data.sensor_type);
                insertCmd.Parameters.AddWithValue("@val", data.value);
                insertCmd.Parameters.AddWithValue("@unit", data.unit);
                insertCmd.Parameters.AddWithValue("@status", data.status);
                await insertCmd.ExecuteNonQueryAsync();

                if (data.status != "OK")
                {
                    var checkCmd = new SqlCommand("SELECT COUNT(*) FROM alarms WHERE machine_id = @mid AND alarm_type = @type AND acknowledged = 0", conn);
                    checkCmd.Parameters.AddWithValue("@mid", machineId);
                    checkCmd.Parameters.AddWithValue("@type", data.sensor_type.ToUpper());
                    int existing = (int)await checkCmd.ExecuteScalarAsync();

                    if (existing == 0)
                    {
                        var alarmCmd = new SqlCommand("INSERT INTO alarms (factory_id, machine_id, alarm_type, severity, message) VALUES (@fid, @mid, @type, @sev, @msg)", conn);
                        alarmCmd.Parameters.AddWithValue("@fid", factoryId);
                        alarmCmd.Parameters.AddWithValue("@mid", machineId);
                        alarmCmd.Parameters.AddWithValue("@type", data.sensor_type.ToUpper());
                        alarmCmd.Parameters.AddWithValue("@sev", data.status == "ALARM" ? "CRITICAL" : "WARNING");
                        alarmCmd.Parameters.AddWithValue("@msg", data.machine + " " + data.sensor_type + ": " + data.value + " " + data.unit);
                        await alarmCmd.ExecuteNonQueryAsync();
                    }
                }

                _logger.LogInformation("Saved: " + data.machine + " " + data.sensor_type + "=" + data.value);
            }
            catch (Exception ex)
            {
                _logger.LogError("Error: " + ex.Message);
            }
        }
    }
}

public class SensorMessage
{
    public string machine { get; set; } = "";
    public string sensor_type { get; set; } = "";
    public double value { get; set; }
    public string unit { get; set; } = "";
    public string status { get; set; } = "OK";
}
