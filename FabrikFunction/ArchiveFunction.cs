using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Microsoft.Data.SqlClient;
using Azure.Storage.Blobs;
using System.Text.Json;

public class ArchiveFunction
{
    private readonly ILogger _logger;
    private readonly string _sqlConn = "Server=sql-fabrikdata.database.windows.net;Database=db-fabrikdata;User Id=sqladmin;Password=Fabrik123!;Encrypt=True;";
    private readonly string _blobConn = Environment.GetEnvironmentVariable("AzureWebJobsStorage") ?? "";

    public ArchiveFunction(ILoggerFactory loggerFactory)
    {
        _logger = loggerFactory.CreateLogger<ArchiveFunction>();
    }

    [Function("ArchiveSensorData")]
    public async Task Run([TimerTrigger("0 0 2 * * *")] TimerInfo timer)
    {
        _logger.LogInformation("Archive job started: " + DateTime.UtcNow);

        using var conn = new SqlConnection(_sqlConn);
        await conn.OpenAsync();

        var selectCmd = new SqlCommand(@"
            SELECT f.name as factory, m.name as machine,
                   s.sensor_type, s.value, s.unit, s.status, s.recorded_at
            FROM sensor_data s
            JOIN machines m ON s.machine_id = m.id
            JOIN factories f ON s.factory_id = f.id
            WHERE s.recorded_at < DATEADD(day, -30, GETUTCDATE())
            ORDER BY s.recorded_at", conn);

        var rows = new List<Dictionary<string, object>>();
        using (var reader = await selectCmd.ExecuteReaderAsync())
        {
            while (await reader.ReadAsync())
            {
                rows.Add(new Dictionary<string, object>
                {
                    ["factory"] = reader["factory"],
                    ["machine"] = reader["machine"],
                    ["sensor_type"] = reader["sensor_type"],
                    ["value"] = reader["value"],
                    ["unit"] = reader["unit"],
                    ["status"] = reader["status"],
                    ["recorded_at"] = reader["recorded_at"]
                });
            }
        }

        if (rows.Count == 0)
        {
            _logger.LogInformation("No data to archive.");
            return;
        }

        var blobClient = new BlobServiceClient(_blobConn);
        var container = blobClient.GetBlobContainerClient("sensor-archive");
        var fileName = "archive-" + DateTime.UtcNow.ToString("yyyy-MM-dd") + ".json";
        var blob = container.GetBlobClient(fileName);
        var json = JsonSerializer.Serialize(rows);
        using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json));
        await blob.UploadAsync(stream, overwrite: true);

        var deleteCmd = new SqlCommand(@"
            DELETE FROM sensor_data
            WHERE recorded_at < DATEADD(day, -30, GETUTCDATE())", conn);
        int deleted = await deleteCmd.ExecuteNonQueryAsync();

        var cutoff = DateTime.UtcNow.AddYears(-5);
        await foreach (var blobItem in container.GetBlobsAsync())
        {
            if (blobItem.Properties.CreatedOn < cutoff)
            {
                await container.DeleteBlobAsync(blobItem.Name);
                _logger.LogInformation("Deleted old archive: " + blobItem.Name);
            }
        }

        _logger.LogInformation("Archived " + rows.Count + " rows, deleted " + deleted + " from SQL.");
    }
}
