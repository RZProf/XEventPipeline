using System.Threading.Channels;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.SqlServer.XEvent.XELite;
using XEventPipeline.Configurations;
using XEventPipeline.XEventSinks.ClickHouse;
using XEventPipeline.XEventSinks.Kafka;
using XEventPipeline.XEventSinks.Postgres;

namespace XEventPipeline;

internal static class Program
{
    private static Task Main(string[] args)
    {
        return CreateHostBuilder(args).Build().RunAsync();
    }

    internal static IHostBuilder CreateHostBuilder(string[] args)
    {
        return Host
            .CreateDefaultBuilder(args)
            .ConfigureAppConfiguration((_, config) =>
            {
                config.Sources.Clear();
                config.AddYamlFile("appsettings.yml", false, true);
            })
            .ConfigureServices((context, services) =>
            {
                services.Configure<SqlServerConfiguration>(context.Configuration.GetRequiredSection("SqlServer"));

                services.Configure<HostOptions>(option =>
                {
                    option.ShutdownTimeout = TimeSpan.FromSeconds(10);
                    option.BackgroundServiceExceptionBehavior = BackgroundServiceExceptionBehavior.StopHost;
                    option.ServicesStartConcurrently = true;
                    option.ServicesStopConcurrently = true;
                });

                services.AddLogging();

                var capacity = int.TryParse(context.Configuration.GetSection("Settings")["BoundedCapacity"],
                    out var boundedCapacity)
                    ? boundedCapacity
                    : 100_000;

                var channel = Channel.CreateBounded<IXEvent>(new BoundedChannelOptions(capacity));
                services.AddSingleton(channel.Writer);
                services.AddSingleton(channel.Reader);

                services.AddSingleton<IXEventSessionManager, XEventSessionManager>();

                services.AddHostedService<XEventStreamer>();

                switch (GetRequestedXEventSinkType(services, context.Configuration))
                {
                    case XEventSinkType.ClickHouse:
                        services.AddHostedService<ClickHouseXEventSink>();
                        break;
                    case XEventSinkType.Postgres:
                        services.AddHostedService<PostgresXEventSink>();
                        break;
                    case XEventSinkType.Kafka:
                        services.AddHostedService<KafkaXEventSink>();
                        break;
                    case XEventSinkType.None:
                    default:
                        throw new InvalidOperationException(
                            "Unable to determine the requested sink type! Make sure you only provided one of the `ClickHouse` or `Postgres` or `Kafka` as your XEventSink.");
                }
            })
            .UseConsoleLifetime();
    }

    private static XEventSinkType GetRequestedXEventSinkType(IServiceCollection services, IConfiguration configuration)
    {
        var clickHouseRegistered = ConfigureOption<ClickHouseConfiguration>(services, configuration, "ClickHouse");
        var postgresRegistered = ConfigureOption<PostgresConfiguration>(services, configuration, "Postgres");
        var kafkaRegistered = ConfigureOption<KafkaConfiguration>(services, configuration, "Kafka");

        return (clickHouseRegistered, postgresRegistered, kafkaRegistered) switch
        {
            (true, false, false) => XEventSinkType.ClickHouse,
            (false, true, false) => XEventSinkType.Postgres,
            (false, false, true) => XEventSinkType.Kafka,
            _ => XEventSinkType.None
        };
    }

    private static bool ConfigureOption<TOptions>(IServiceCollection services, IConfiguration configuration, string key)
        where TOptions : class
    {
        try
        {
            services.Configure<TOptions>(configuration.GetRequiredSection(key));
            return true;
        }
        catch
        {
            return false;
        }
    }
}