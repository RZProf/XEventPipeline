using System.Threading.Channels;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.SqlServer.XEvent.XELite;
using Polly;
using Polly.Retry;
using XEventPipeline.Configurations;

namespace XEventPipeline;

public class XEventStreamer : IHostedLifecycleService
{
    private readonly CancellationTokenSource _cancellationTokenSource;
    private readonly SqlServerConfiguration _configuration;
    private readonly ILogger<XEventStreamer> _logger;
    private readonly ResiliencePipeline _resiliencePipeline;
    private readonly IXEventSessionManager _xEventSessionManager;
    private readonly ChannelWriter<IXEvent> _xEventWriter;

    private Task? _streamingLoop;

    public XEventStreamer(
        IOptions<SqlServerConfiguration> configuration,
        IXEventSessionManager xEventSessionManager,
        ChannelWriter<IXEvent> xEventWriter,
        ILogger<XEventStreamer> logger)
    {
        _configuration = configuration.Value;
        _xEventSessionManager = xEventSessionManager;
        _xEventWriter = xEventWriter;
        _logger = logger;

        _cancellationTokenSource = new CancellationTokenSource();

        _resiliencePipeline = new ResiliencePipelineBuilder()
            .AddRetry(new RetryStrategyOptions
            {
                ShouldHandle = new PredicateBuilder().Handle<Exception>(ex =>
                {
                    return ex switch
                    {
                        OperationCanceledException or TaskCanceledException => false,
                        SqlException sqlEx => sqlEx.Number != 0,
                        _ => true
                    };
                }),
                MaxRetryAttempts = int.MaxValue,
                BackoffType = DelayBackoffType.Exponential,
                UseJitter = true,
                Delay = TimeSpan.FromSeconds(2),
                MaxDelay = TimeSpan.FromSeconds(30),
                OnRetry = async args =>
                {
                    _logger.LogError(
                        args.Outcome.Exception,
                        "XEvent stream connection lost. Retrying in {Delay:g} (attempt #{Attempt}).",
                        args.RetryDelay,
                        args.AttemptNumber + 1);

                    await _xEventSessionManager.MakeSureSessionIsAlive(args.Context.CancellationToken);
                }
            })
            .Build();
    }

    public Task StartingAsync(CancellationToken cancellationToken)
    {
        return _xEventSessionManager.InitializeSession(cancellationToken);
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _streamingLoop = Task.Run(async () => await StreamEventsAsync(), cancellationToken);

        return Task.CompletedTask;
    }

    public Task StartedAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }

    public Task StoppingAsync(CancellationToken cancellationToken)
    {
        _xEventWriter.TryComplete();
        return _cancellationTokenSource.CancelAsync();
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        await _xEventSessionManager.DropSession(cancellationToken);

        if (_streamingLoop != null)
            try
            {
                await _streamingLoop;
            }
            catch (Exception e) when (e is TaskCanceledException or OperationCanceledException)
            {
                // Expected on cancellation
            }
            finally
            {
                _streamingLoop.Dispose();
            }

        _cancellationTokenSource.Dispose();
    }

    public Task StoppedAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }

    private async Task StreamEventsAsync()
    {
        try
        {
            await _resiliencePipeline.ExecuteAsync(static async (state, cancellationToken) =>
            {
                var (xEventWriter, logger, config) = state;

                var streamer = new XELiveEventStreamer(config.ConnectionString, config.SessionName);

                await streamer.ReadEventStream(
                    () =>
                    {
                        logger.LogInformation("Connected to SQL Server XEvent stream. Listening for events.");
                        return Task.CompletedTask;
                    },
                    async xeEvent => { await xEventWriter.WriteAsync(xeEvent, cancellationToken); },
                    cancellationToken);
            }, (XEventWriter: _xEventWriter, Logger: _logger, Config: _configuration), _cancellationTokenSource.Token);
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("XEvent stream stopped gracefully.");
        }
        catch (SqlException sqlEx) when (sqlEx.Number == 0)
        {
            _logger.LogInformation("XEvent stream stopped gracefully.");
        }
        catch (Exception e)
        {
            _logger.LogError(e, "Unhandled exception in XEvent stream loop.");
        }
    }
}