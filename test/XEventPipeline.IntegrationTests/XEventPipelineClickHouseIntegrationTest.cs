using System.Text.RegularExpressions;
using Microsoft.Extensions.Configuration;
using Testcontainers.ClickHouse;
using Testcontainers.MsSql;
using XEventPipeline.Configurations;

namespace XEventPipeline.IntegrationTests;

public class XEventPipelineClickHouseIntegrationTest
{
    private readonly ClickHouseContainer _clickHouseContainer =
        new ClickHouseBuilder("clickhouse/clickhouse-server:26.3-alpine").Build();

    private readonly MsSqlContainer _msSqlContainer =
        new MsSqlBuilder("mcr.microsoft.com/mssql/server:2025-latest").Build();

    [Before(Test)]
    public async Task Before()
    {
        await Task.WhenAll(_clickHouseContainer.StartAsync(), _msSqlContainer.StartAsync());
    }

    [Test]
    [Timeout(240_000)]
    [Arguments(ClickHouseCompression.None)]
    [Arguments(ClickHouseCompression.GZip)]
    public async Task Overall(ClickHouseCompression compression, CancellationToken cancellationToken)
    {
        var clickHouseConnectionString = _clickHouseContainer.GetConnectionString();
        var msSqlConnectionString = _msSqlContainer.GetConnectionString();
        
        var table = $"xe_data_{compression.ToString("G").ToLowerInvariant()}";
        
        var testSettings = new Dictionary<string, string?>
        {
            { "Settings:BoundedCapacity", "100000" },
            { "ClickHouse:BatchSize", "10" },
            { "ClickHouse:ConnectionString", clickHouseConnectionString },
            { "ClickHouse:Compression", compression.ToString() },
            { "ClickHouse:Table", table },
            { "SqlServer:ConnectionString", msSqlConnectionString },
            { "SqlServer:Events:0:Package", "sqlserver" },
            { "SqlServer:Events:0:Name", "sp_statement_completed" },
            { "SqlServer:Events:0:PredicateExpression", "[duration]>= 5000000" },
            { "SqlServer:Events:0:CustomizableAttributes:0:Name", "collect_statement" },
            { "SqlServer:Events:0:CustomizableAttributes:0:Value", "1" },
            { "SqlServer:Events:0:Actions:0", "client_app_name" },
            { "SqlServer:Events:0:Actions:1", "client_hostname" },
            { "SqlServer:Events:0:Actions:2", "database_name" },
            { "SqlServer:Events:0:Actions:3", "query_hash" },
            { "SqlServer:Events:0:Actions:4", "username" },
            { "SqlServer:Events:1:Package", "sqlserver" },
            { "SqlServer:Events:1:Name", "sql_batch_completed" },
            { "SqlServer:Events:1:PredicateExpression", "[duration]>= 5000000" },
            { "SqlServer:Events:1:CustomizableAttributes:0:Name", "collect_batch_text" },
            { "SqlServer:Events:1:CustomizableAttributes:0:Value", "1" },
            { "SqlServer:Events:1:Actions:0", "client_app_name" },
            { "SqlServer:Events:1:Actions:1", "client_hostname" },
            { "SqlServer:Events:1:Actions:2", "database_name" },
            { "SqlServer:Events:1:Actions:3", "query_hash" },
            { "SqlServer:Events:1:Actions:4", "username" }
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
        var persistedXEvents = await ClickHouseDataReader.ReadPersistedXEvents(
            clickHouseConnectionString,
            table,
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
        await Task.WhenAll(_clickHouseContainer.DisposeAsync().AsTask(), _msSqlContainer.DisposeAsync().AsTask());
    }
}