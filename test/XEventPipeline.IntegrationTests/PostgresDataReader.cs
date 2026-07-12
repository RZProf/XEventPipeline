using System.Text.Json;
using ClickHouse.Driver.Utility;
using Npgsql;

namespace XEventPipeline.IntegrationTests;

public static class PostgresDataReader
{
    public static async Task<PersistedXEvent[]> ReadPersistedXEvents(
        string connectionString,
        string table,
        int count,
        CancellationToken cancellationToken)
    {
        var recordsAffected = 0;
        var data = new HashSet<PersistedXEvent>();

        while (!cancellationToken.IsCancellationRequested && recordsAffected < count)
        {
            await using NpgsqlConnection npgsqlConnection = new(connectionString);
            await npgsqlConnection.OpenAsync(cancellationToken);
            var reader = await npgsqlConnection.ExecuteReaderAsync($"SELECT * FROM {table};");

            while (await reader.ReadAsync(cancellationToken))
                if (data.Add(new PersistedXEvent
                    {
                        Uuid = reader.GetGuid(1),
                        Name = reader.GetString(2),
                        Timestamp = reader.GetDateTime(3),
                        XEventStartOffsetInBytes = reader.GetInt64(4),
                        XEventEndOffsetInBytes = reader.GetInt64(5),
                        XEventSizeInBytes = reader.GetInt64(6),
                        Actions = JsonSerializer.Deserialize<Dictionary<string, object>>(reader.GetString(7)) ??
                                  new Dictionary<string, object>(),
                        Fields = JsonSerializer.Deserialize<Dictionary<string, object>>(reader.GetString(8)) ??
                                 new Dictionary<string, object>()
                    }))
                    recordsAffected++;
        }

        return data.ToArray();
    }
}