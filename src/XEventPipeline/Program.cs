using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using XEventPipeline.Configurations;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Components.Server.Circuits;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.Extensions.FileProviders;
using System.Diagnostics;
using XEventPipeline.Components;
using XEventPipeline.Setup;

namespace XEventPipeline;

internal static class Program
{
    private static async Task Main(string[] args)
    {
        var configure = args.Contains("--configure", StringComparer.OrdinalIgnoreCase);
        var hostArgs = args.Where(argument => !argument.Equals("--configure", StringComparison.OrdinalIgnoreCase)).ToArray();
        if (!configure)
        {
            await CreateHostBuilder(hostArgs).Build().RunAsync();
            return;
        }

        var appSettingsPath = AppSettingsFile.ResolvePath();
        var builder = WebApplication.CreateBuilder(hostArgs);
        builder.Configuration.Sources.Clear();
        try
        {
            builder.Configuration.AddYamlFile(appSettingsPath, optional: true, reloadOnChange: false);
        }
        catch (Exception ex)
        {
            const string warning = "The existing appsettings.yml could not be read and was ignored. Saving will replace it.";
            Console.Error.WriteLine($"{warning} Details: {ex.Message}");
            builder.Configuration.Sources.Clear();
            builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Setup:ConfigurationWarning"] = warning
            });
        }
        builder.Services.AddRazorComponents().AddInteractiveServerComponents();
        builder.Services.AddScoped<XEventCatalogService>();
        builder.Services.AddSingleton(new AppSettingsFile(appSettingsPath));
        builder.Services.AddSingleton<ConfigureCircuitActivity>();
        builder.Services.AddScoped<CircuitHandler, ConfigureCircuitHandler>();
        builder.Services.AddHostedService<ConfigureCircuitLifetimeService>();

        var app = builder.Build();
        app.UseStaticFiles(new StaticFileOptions
        {
            FileProvider = new CompositeFileProvider(
                app.Environment.WebRootFileProvider,
                new PhysicalFileProvider(Path.Combine(AppContext.BaseDirectory, "wwwroot")))
        });
        app.UseAntiforgery();
        app.MapRazorComponents<App>().AddInteractiveServerRenderMode();

        await app.StartAsync();
        OpenConfigurePage(app);
        await app.WaitForShutdownAsync();
    }

    private static void OpenConfigurePage(WebApplication app)
    {
        var addresses = app.Services.GetRequiredService<IServer>()
            .Features.Get<IServerAddressesFeature>()?.Addresses;
        var address = addresses?.Select(ToLoopbackAddress).FirstOrDefault(x => x is not null);
        if (address is null)
        {
            app.Logger.LogWarning("Could not determine the configure UI address. Open the local URL printed above.");
            return;
        }

        try
        {
            Process.Start(new ProcessStartInfo(address) { UseShellExecute = true });
        }
        catch (Exception ex)
        {
            app.Logger.LogWarning(ex, "Could not open a browser automatically. Open {ConfigureUrl} manually.", address);
        }
    }

    private static string? ToLoopbackAddress(string address)
    {
        if (!Uri.TryCreate(address, UriKind.Absolute, out var uri)) return null;
        var host = uri.Host is "0.0.0.0" or "*" or "+" or "[::]" or "::" ? "localhost" : uri.Host;
        return new UriBuilder(uri) { Host = host }.Uri.ToString();
    }

    internal static IHostBuilder CreateHostBuilder(string[] args)
    {
        var appSettingsPath = AppSettingsFile.ResolvePath();
        var builder = Host
            .CreateDefaultBuilder(args)
            .ConfigureAppConfiguration((_, config) =>
            {
                config.Sources.Clear();
                config.AddYamlFile(appSettingsPath, false, true);
            })
            .ConfigureServices((context, services) =>
            {
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

                services.Configure<SqlServerConfiguration>(context.Configuration.GetRequiredSection("SqlServer"));
                services.ConfigureXEventBuffer(context.Configuration);
                services.AddSingleton<IXEventSessionManager, XEventSessionManager>();
                services.AddHostedService<XEventStreamer>();
                services.AddXEventSink(context.Configuration);
                services.ConfigureOtel(context.Configuration);
            });

        return builder.UseConsoleLifetime();
    }
}
