using Microsoft.AspNetCore.Components.Server.Circuits;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace XEventPipeline;

internal sealed class ConfigureCircuitActivity
{
    private readonly object _lock = new();
    private readonly HashSet<string> _connectedCircuitIds = [];
    private bool _hasHadConnection;
    private long _disconnectedUtcTicks;

    public void Connected(string circuitId)
    {
        lock (_lock)
        {
            _connectedCircuitIds.Add(circuitId);
            _hasHadConnection = true;
            _disconnectedUtcTicks = 0;
        }
    }

    public void Disconnected(string circuitId)
    {
        lock (_lock)
        {
            if (!_connectedCircuitIds.Remove(circuitId)) return;
            if (_connectedCircuitIds.Count == 0)
                _disconnectedUtcTicks = DateTime.UtcNow.Ticks;
        }
    }

    public (bool HasHadConnection, int ActiveConnections, long DisconnectedUtcTicks) GetSnapshot()
    {
        lock (_lock)
            return (_hasHadConnection, _connectedCircuitIds.Count, _disconnectedUtcTicks);
    }
}

internal sealed class ConfigureCircuitHandler(ConfigureCircuitActivity activity) : CircuitHandler
{
    public override Task OnConnectionUpAsync(Circuit circuit, CancellationToken cancellationToken)
    {
        activity.Connected(circuit.Id);
        return Task.CompletedTask;
    }

    public override Task OnConnectionDownAsync(Circuit circuit, CancellationToken cancellationToken)
    {
        activity.Disconnected(circuit.Id);
        return Task.CompletedTask;
    }

    public override Task OnCircuitClosedAsync(Circuit circuit, CancellationToken cancellationToken)
    {
        activity.Disconnected(circuit.Id);
        return Task.CompletedTask;
    }
}

internal sealed class ConfigureCircuitLifetimeService(
    ConfigureCircuitActivity activity,
    IHostApplicationLifetime lifetime,
    ILogger<ConfigureCircuitLifetimeService> logger) : BackgroundService
{
    private static readonly TimeSpan CheckInterval = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan DisconnectGracePeriod = TimeSpan.FromSeconds(3);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(CheckInterval);
        while (await timer.WaitForNextTickAsync(stoppingToken))
        {
            var snapshot = activity.GetSnapshot();
            if (!snapshot.HasHadConnection || snapshot.ActiveConnections > 0 || snapshot.DisconnectedUtcTicks == 0)
                continue;

            var disconnectedAt = new DateTime(snapshot.DisconnectedUtcTicks, DateTimeKind.Utc);
            if (DateTime.UtcNow - disconnectedAt < DisconnectGracePeriod)
                continue;

            logger.LogInformation("No configure UI circuits reconnected within {GracePeriod}. Stopping configure host.", DisconnectGracePeriod);
            lifetime.StopApplication();
            return;
        }
    }
}
