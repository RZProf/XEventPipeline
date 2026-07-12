using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using XEventPipeline.Configurations;

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

                services.AddLogging(builder =>
                {
                    builder.AddConfiguration(context.Configuration.GetSection("Logging"));
                });

                services.ConfigureXEventBuffer(context.Configuration);

                services.AddSingleton<IXEventSessionManager, XEventSessionManager>();
                services.AddHostedService<XEventStreamer>();
                services.AddXEventSink(context.Configuration);

                services.ConfigureOtel(context.Configuration);
            })
            .UseConsoleLifetime();
    }
}