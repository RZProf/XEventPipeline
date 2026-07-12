using System.Text.RegularExpressions;
using Microsoft.Extensions.Configuration;
using Testcontainers.MsSql;
using Testcontainers.PostgreSql;

namespace XEventPipeline.IntegrationTests;

public class XEventPipelinePostgresIntegrationTest
{
    private readonly MsSqlContainer _msSqlContainer =
        new MsSqlBuilder("mcr.microsoft.com/mssql/server:2025-latest").Build();

    private readonly PostgreSqlContainer _postgresContainer = new PostgreSqlBuilder("postgres:16").Build();

    [Before(Test)]
    public async Task Before()
    {
        await Task.WhenAll(_postgresContainer.StartAsync(), _msSqlContainer.StartAsync());
    }

    [Test]
    [Timeout(240_000)]
    public async Task Overall(CancellationToken cancellationToken)
    {
        var msSqlConnectionString = _msSqlContainer.GetConnectionString();
        var postgresConnectionString = _postgresContainer.GetConnectionString();

        var testSettings = new Dictionary<string, string?>
        {
            { "Settings:BoundedCapacity", "100000" },
            { "Postgres:BatchSize", "10" },
            { "Postgres:ConnectionString", postgresConnectionString },
            { "SqlServer:ConnectionString", msSqlConnectionString },
            { "SqlServer:Events:0:Name", "sqlserver.sp_statement_completed" },
            { "SqlServer:Events:0:PredicateExpression", "[duration]>= 5000000" },
            { "SqlServer:Events:0:CustomizableAttributes:0:Name", "collect_statement" },
            { "SqlServer:Events:0:CustomizableAttributes:0:Value", "1" },
            { "SqlServer:Events:0:Actions:0", "sqlserver.client_app_name" },
            { "SqlServer:Events:0:Actions:1", "sqlserver.client_hostname" },
            { "SqlServer:Events:0:Actions:2", "sqlserver.database_name" },
            { "SqlServer:Events:0:Actions:3", "sqlserver.query_hash" },
            { "SqlServer:Events:0:Actions:4", "sqlserver.username" },
            { "SqlServer:Events:1:Name", "sqlserver.sql_batch_completed" },
            { "SqlServer:Events:1:PredicateExpression", "[duration]>= 5000000" },
            { "SqlServer:Events:1:CustomizableAttributes:0:Name", "collect_batch_text" },
            { "SqlServer:Events:1:CustomizableAttributes:0:Value", "1" },
            { "SqlServer:Events:1:Actions:0", "sqlserver.client_app_name" },
            { "SqlServer:Events:1:Actions:1", "sqlserver.client_hostname" },
            { "SqlServer:Events:1:Actions:2", "sqlserver.database_name" },
            { "SqlServer:Events:1:Actions:3", "sqlserver.query_hash" },
            { "SqlServer:Events:1:Actions:4", "sqlserver.username" }
        };

        var host = Program.CreateHostBuilder([])
            .ConfigureAppConfiguration((_, configurationBuilder) =>
            {
                configurationBuilder.Sources.Clear();
                configurationBuilder.AddInMemoryCollection(testSettings);
            })
            .Build();

        _ = Task.Run(async () => await host.StartAsync(cancellationToken), cancellationToken);

        await Task.Delay(1000, cancellationToken);

        await MsSqlQueryRunner.RunWaitForDelays(msSqlConnectionString, 10, cancellationToken);
        var persistedXEvents = await PostgresDataReader.ReadPersistedXEvents(
            postgresConnectionString,
            "xe_data",
            10,
            cancellationToken);

        await host.StopAsync(cancellationToken);

        await Assert.That(persistedXEvents.Length).IsEqualTo(10);
        await Assert
            .That(persistedXEvents
                .All(xe => Regex.IsMatch(xe.Fields["batch_text"].ToString()!, "WAITFOR DELAY '0:00:(0?[6-9]|10)'")))
            .IsTrue();
    }

    [After(Test)]
    public async Task After()
    {
        await Task.WhenAll(_postgresContainer.DisposeAsync().AsTask(), _msSqlContainer.DisposeAsync().AsTask());
    }
}