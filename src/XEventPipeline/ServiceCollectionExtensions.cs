using ClickHouse.Driver.Diagnostic;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using OpenTelemetry.Logs;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using XEventPipeline.Configurations;
using XEventPipeline.XEventSinks.ClickHouse;
using XEventPipeline.XEventSinks.Kafka;
using XEventPipeline.XEventSinks.Postgres;

namespace XEventPipeline;

public static class ServiceCollectionExtensions
{
    public static void ConfigureXEventBuffer(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        var capacity = int.TryParse(configuration.GetSection("Settings")["BoundedCapacity"],
            out var boundedCapacity)
            ? boundedCapacity
            : 100_000;

        var channel = new XEventBuffer.XEventBuffer(capacity);
        services.AddSingleton(channel.Writer);
        services.AddSingleton(channel.Reader);

        services.AddSingleton(new XEventPipelineDiagnostics(channel.Reader));
    }

    public static void ConfigureOtel(this IServiceCollection services, IConfiguration configuration)
    {
        var xEventSinkType = GetRequestedXEventSinkType(configuration);

        services.AddOpenTelemetry()
            .ConfigureResource(resource => resource.AddService("xe_pipeline"))
            .WithTracing(tracing =>
            {
                tracing.AddSqlClientInstrumentation();

                switch (xEventSinkType)
                {
                    case XEventSinkType.ClickHouse:
                        tracing.AddSource(ClickHouseDiagnosticsOptions.ActivitySourceName)
                            .AddHttpClientInstrumentation();
                        break;
                    case XEventSinkType.Postgres:
                        tracing.AddNpgsql();
                        break;
                    case XEventSinkType.Kafka:
                        tracing.AddSource("Confluent.Kafka.Extensions.Diagnostics");
                        break;
                    case XEventSinkType.None:
                    default:
                        throw new InvalidOperationException(
                            "Unable to determine the requested sink type! Make sure you only provided one of the `ClickHouse` or `Postgres` or `Kafka` as your XEventSink.");
                }

                tracing.AddOtlpExporter();
            })
            .WithMetrics(metrics =>
            {
                metrics
                    .AddMeter(XEventPipelineDiagnostics.Name)
                    .AddRuntimeInstrumentation()
                    .AddProcessInstrumentation()
                    .AddSqlClientInstrumentation();

                switch (xEventSinkType)
                {
                    case XEventSinkType.ClickHouse:
                        metrics.AddHttpClientInstrumentation();
                        break;
                    case XEventSinkType.Postgres:
                        metrics.AddNpgsqlInstrumentation();
                        break;
                    case XEventSinkType.Kafka:
                        break;
                    case XEventSinkType.None:
                    default:
                        throw new InvalidOperationException(
                            "Unable to determine the requested sink type! Make sure you only provided one of the `ClickHouse` or `Postgres` or `Kafka` as your XEventSink.");
                }

                metrics.AddOtlpExporter();
            })
            .WithLogging(
                logs => { logs.AddOtlpExporter(); },
                options =>
                {
                    options.IncludeFormattedMessage = true;
                    options.IncludeScopes = true;
                });
    }

    public static void AddXEventSink(this IServiceCollection services, IConfiguration configuration)
    {
        var xEventSinkType = GetRequestedXEventSinkType(configuration);

        switch (xEventSinkType)
        {
            case XEventSinkType.ClickHouse:
                services.Configure<ClickHouseConfiguration>(configuration.GetRequiredSection("ClickHouse"));
                services.AddHostedService<ClickHouseXEventSink>();
                break;
            case XEventSinkType.Postgres:
                services.Configure<PostgresConfiguration>(configuration.GetRequiredSection("Postgres"));
                services.AddHostedService<PostgresXEventSink>();
                break;
            case XEventSinkType.Kafka:
                services.Configure<KafkaConfiguration>(configuration.GetRequiredSection("Kafka"));
                services.AddHostedService<KafkaXEventSink>();
                break;
            case XEventSinkType.None:
            default:
                throw new InvalidOperationException(
                    "Unable to determine the requested sink type! Make sure you only provided one of the `ClickHouse` or `Postgres` or `Kafka` as your XEventSink.");
        }
    }

    private static XEventSinkType GetRequestedXEventSinkType(IConfiguration configuration)
    {
        var clickHouseRegistered = SectionExists(configuration, "ClickHouse");
        var postgresRegistered = SectionExists(configuration, "Postgres");
        var kafkaRegistered = SectionExists(configuration, "Kafka");

        return (clickHouseRegistered, postgresRegistered, kafkaRegistered) switch
        {
            (true, false, false) => XEventSinkType.ClickHouse,
            (false, true, false) => XEventSinkType.Postgres,
            (false, false, true) => XEventSinkType.Kafka,
            _ => XEventSinkType.None
        };
    }

    private static bool SectionExists(IConfiguration configuration, string key)
    {
        try
        {
            _ = configuration.GetRequiredSection(key);
            return true;
        }
        catch
        {
            return false;
        }
    }
}
