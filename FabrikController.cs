using Microsoft.AspNetCore.Mvc;
using Microsoft.Data.SqlClient;
 
[ApiController]
[Route("api/[controller]")]
public class FabrikController : ControllerBase
{
    private readonly string _conn = $"Server=sql-fabrikdata.database.windows.net;Database=db-fabrikdata;User Id=sqladmin;Password={Environment.GetEnvironmentVariable("SqlPassword") ?? "Fabrik123!"};Encrypt=True;";
 
    private List<Dictionary<string, object>> Query(string sql, SqlParameter[]? parameters = null)
    {
        var result = new List<Dictionary<string, object>>();
        using var conn = new SqlConnection(_conn);
        conn.Open();
        using var cmd = new SqlCommand(sql, conn);
        if (parameters != null) cmd.Parameters.AddRange(parameters);
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            var row = new Dictionary<string, object>();
            for (int i = 0; i < reader.FieldCount; i++)
                row[reader.GetName(i)] = reader.IsDBNull(i) ? "" : reader.GetValue(i);
            result.Add(row);
        }
        return result;
    }
 
    [HttpGet("factories")]
    public IActionResult GetFactories() =>
        Ok(Query("SELECT id, name, location, country FROM factories WHERE active = 1"));
 
    [HttpGet("sensors")]
    public IActionResult GetSensors([FromQuery] int factoryId) =>
        Ok(Query(@"
            SELECT f.name as factory, m.name as machine, s.sensor_type, s.value, s.unit, s.status, s.recorded_at
            FROM sensor_data s
            JOIN machines m ON s.machine_id = m.id
            JOIN factories f ON s.factory_id = f.id
            WHERE s.factory_id = @fid
            AND s.recorded_at = (
                SELECT MAX(s2.recorded_at) FROM sensor_data s2
                WHERE s2.machine_id = s.machine_id AND s2.sensor_type = s.sensor_type
            )
            ORDER BY s.recorded_at DESC",
            new[] { new SqlParameter("@fid", factoryId) }));
 
    [HttpGet("alarms")]
    public IActionResult GetAlarms([FromQuery] int factoryId) =>
        Ok(Query(@"
            SELECT a.id, m.name as machine, a.alarm_type, a.severity, a.message, a.acknowledged, a.created_at
            FROM alarms a
            JOIN machines m ON a.machine_id = m.id
            WHERE a.factory_id = @fid AND a.acknowledged = 0
            ORDER BY a.created_at DESC",
            new[] { new SqlParameter("@fid", factoryId) }));
 
    [HttpPost("alarms/{id}/acknowledge")]
    public IActionResult AcknowledgeAlarm(int id)
    {
        using var conn = new SqlConnection(_conn);
        conn.Open();
        var cmd = new SqlCommand(@"
            UPDATE alarms 
            SET acknowledged = 1, acknowledged_at = GETUTCDATE(), acknowledged_by = 'Dashboard User'
            WHERE id = @id", conn);
        cmd.Parameters.AddWithValue("@id", id);
        int rows = cmd.ExecuteNonQuery();
        if (rows == 0) return NotFound();
        return Ok(new { success = true });
    }
 
    [HttpGet("production")]
    public IActionResult GetProduction([FromQuery] int factoryId) =>
        Ok(Query(@"
            SELECT m.name as machine, p.shift, p.units_produced, p.units_rejected, p.runtime_min, p.downtime_min
            FROM production p
            JOIN machines m ON p.machine_id = m.id
            WHERE p.factory_id = @fid",
            new[] { new SqlParameter("@fid", factoryId) }));
 
    [HttpGet("overview")]
    public IActionResult GetOverview() =>
        Ok(Query(@"
            SELECT f.id, f.name, f.location,
                (SELECT COUNT(DISTINCT id) FROM machines WHERE factory_id = f.id) as machine_count,
                (SELECT COUNT(*) FROM alarms WHERE factory_id = f.id AND acknowledged = 0) as active_alarms,
                (SELECT SUM(units_produced) FROM production WHERE factory_id = f.id) as total_units
            FROM factories f
            WHERE f.active = 1"));
}
