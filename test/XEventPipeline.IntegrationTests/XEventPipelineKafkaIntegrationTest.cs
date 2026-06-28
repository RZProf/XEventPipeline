using System.Text.Json;
using System.Text.RegularExpressions;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Configuration;
using Testcontainers.Kafka;
using Testcontainers.MsSql;

namespace XEventPipeline.IntegrationTests;

public class XEventPipelineKafkaIntegrationTest
{
    private readonly KafkaContainer _kafkaContainer = new KafkaBuilder("confluentinc/cp-kafka:7.4.0").Build();

    private readonly MsSqlContainer _msSqlContainer =
        new MsSqlBuilder("mcr.microsoft.com/mssql/server:2025-latest").Build();

    [Before(Test)]
    public async Task Before()
    {
        await Task.WhenAll(_kafkaContainer.StartAsync(), _msSqlContainer.StartAsync());
    }

    [Test]
    [Timeout(240_000)]
    public async Task Overall(CancellationToken cancellationToken)
    {
        var bootstrapServers = _kafkaContainer.GetBootstrapAddress();
        var msSqlConnectionString = _msSqlContainer.GetConnectionString();
        
        var topic = await CreateRandomTopic(bootstrapServers);

        var testSettings = new Dictionary<string, string?>
        {
            { "Settings:BoundedCapacity", "100000" },
            { "Kafka:BrokerAddress", bootstrapServers },
            { "Kafka:Topic", topic },
            { "Kafka:CompressionType", "Gzip" },
            { "Kafka:LingerMs", "5" },
            { "Kafka:BatchSize", "10" },
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
        var persistedXEvents = Consume(bootstrapServers, topic, 10, cancellationToken).ToArray();

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
        await Task.WhenAll(_kafkaContainer.DisposeAsync().AsTask(), _msSqlContainer.DisposeAsync().AsTask());
    }

    private static async Task<string> CreateRandomTopic(string bootstrapServers)
    {
        var topic = DateTime.Now.Ticks.ToString();
        AdminClientConfig adminClientConfig = new() { BootstrapServers = bootstrapServers };
        var adminClient = new AdminClientBuilder(adminClientConfig).Build();

        await adminClient.CreateTopicsAsync([
            new TopicSpecification
            {
                Name = topic,
                NumPartitions = 1,
                ReplicationFactor = 1
            }
        ]);

        adminClient.Dispose();

        return topic;
    }

    private static IEnumerable<PersistedXEvent?> Consume(
        string bootstrapServers,
        string topic, int count,
        CancellationToken cancellationToken)
    {
        ConsumerConfig consumerConfig = new()
        {
            GroupId = Guid.NewGuid().ToString(),
            BootstrapServers = bootstrapServers,
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        using var consumer = new ConsumerBuilder<Null, string>(consumerConfig).Build();
        consumer.Subscribe(topic);

        var consumed = 0;

        while (consumed < count)
        {
            var consumeResult = consumer.Consume(cancellationToken);

            if (consumeResult is null) 
                continue;

            yield return JsonSerializer.Deserialize<PersistedXEvent>(consumeResult.Message.Value);

            consumed++;
        }
    }
}