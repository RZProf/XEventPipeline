using Microsoft.Data.SqlClient;

namespace XEventPipeline.IntegrationTests;

public abstract class MsSqlQueryRunner
{
    public static async Task RunWaitForDelays(string connectionString, int count, CancellationToken cancellationToken)
    {
        await Task.WhenAll(Enumerable.Range(0, count).Select(async _ =>
        {
            var sqlConnectionStringBuilder = new SqlConnectionStringBuilder(connectionString)
            {
                ApplicationName = "XEventPipeline.IntegrationTests",
                ApplicationIntent = ApplicationIntent.ReadOnly
            };

            await using var sqlConnection = new SqlConnection(sqlConnectionStringBuilder.ConnectionString);
            await sqlConnection.OpenAsync(cancellationToken);

            var delay = TimeSpan.FromSeconds(Random.Shared.Next(6, 10)).ToString("g");
            var command = sqlConnection.CreateCommand();
            command.CommandText = $"WAITFOR DELAY '{delay}';";

            await command.ExecuteNonQueryAsync(cancellationToken);
        }));
    }
}