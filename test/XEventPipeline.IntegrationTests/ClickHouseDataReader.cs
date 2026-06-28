using System.Data.Common;
using System.Runtime.InteropServices.JavaScript;
using System.Text.Json;
using ClickHouse.Driver;

namespace XEventPipeline.IntegrationTests;

public static class ClickHouseDataReader
{
    public static async Task<PersistedXEvent[]> ReadPersistedXEvents(
        string connectionString,
        string table,
        int count,
        CancellationToken cancellationToken)
    {
        using ClickHouseClient connection = new(connectionString);

        var recordsAffected = 0;
        var data = new HashSet<PersistedXEvent>();

        while (!cancellationToken.IsCancellationRequested && recordsAffected < count)
        {
            await using DbDataReader? reader =
                await connection.ExecuteReaderAsync($"SELECT * FROM {table} ORDER BY `UUID`", null, null,
                    cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                if (data.Add(new PersistedXEvent
                    {
                        Uuid = reader.GetGuid(0),
                        Name = reader.GetString(1),
                        Timestamp = reader.GetDateTime(2),
                        XEventStartOffsetInBytes = reader.GetInt64(3),
                        XEventEndOffsetInBytes = reader.GetInt64(4),
                        XEventSizeInBytes = reader.GetInt64(5),
                        Actions = JsonSerializer.Deserialize<Dictionary<string, object>>(reader.GetString(6)) ??
                                  new Dictionary<string, object>(),
                        Fields = JsonSerializer.Deserialize<Dictionary<string, object>>(reader.GetString(7)) ??
                                 new Dictionary<string, object>()
                    }))
                {
                    recordsAffected++;
                }
            }
        }

        return data.ToArray();
    }
}