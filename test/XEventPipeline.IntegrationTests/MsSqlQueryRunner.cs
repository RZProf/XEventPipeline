using Microsoft.Data.SqlClient;

namespace XEventPipeline.IntegrationTests;

public abstract class MsSqlQueryRunner
{
    public static async Task RunWaitForDelays(string connectionString, int count, CancellationToken cancellationToken)
    {
        await Task.WhenAll(Enumerable.Range(0, count).Select(async _ =>
        {
            await using var sqlConnection = new SqlConnection(connectionString);
            await sqlConnection.OpenAsync(cancellationToken);

            var delay = TimeSpan.FromSeconds(Random.Shared.Next(6, 10)).ToString("g");
            var command = sqlConnection.CreateCommand();
            command.CommandText = $"WAITFOR DELAY '{delay}';";

            await command.ExecuteNonQueryAsync(cancellationToken);
        }));
    }
}