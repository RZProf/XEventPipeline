using System.Buffers;
using System.IO.Compression;
using System.IO.Pipelines;
using System.Threading.Channels;
using ClickHouse.Driver;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.SqlServer.XEvent.XELite;
using Polly;
using Polly.Retry;
using XEventPipeline.Configurations;

namespace XEventPipeline.XEventSinks.ClickHouse;

public class ClickHouseXEventSink : IHostedLifecycleService
{
    private static readonly PipeOptions PipeOptions = new(
        MemoryPool<byte>.Shared,
        pauseWriterThreshold: 1024 * 1024 * 4,
        resumeWriterThreshold: 1024 * 1024 * 2,
        useSynchronizationContext: false);

    private readonly CancellationTokenSource _cancellationTokenSource;
    private readonly ClickHouseConfiguration _configuration;
    private readonly ILogger<ClickHouseXEventSink> _logger;
    private readonly ResiliencePipeline _resiliencePipeline;

    private readonly ChannelReader<IXEvent> _xEventReader;

    private Task? _insertBackgroundTask;

    public ClickHouseXEventSink(
        IOptions<ClickHouseConfiguration> configuration,
        ChannelReader<IXEvent> xEventReader,
        ILogger<ClickHouseXEventSink> logger)
    {
        _configuration = configuration.Value;
        _xEventReader = xEventReader;
        _logger = logger;

        _cancellationTokenSource = new CancellationTokenSource();

        _resiliencePipeline = new ResiliencePipelineBuilder()
            .AddRetry(new RetryStrategyOptions
            {
                ShouldHandle = new PredicateBuilder().Handle<Exception>(ex =>
                    ex is not OperationCanceledException and not TaskCanceledException),
                MaxRetryAttempts = int.MaxValue,
                BackoffType = DelayBackoffType.Exponential,
                UseJitter = true,
                Delay = TimeSpan.FromSeconds(2),
                MaxDelay = TimeSpan.FromSeconds(30),
                OnRetry = args =>
                {
                    _logger.LogError(
                        args.Outcome.Exception,
                        "Failed to insert XEvents into ClickHouse. Retrying in {Delay:g} (attempt #{Attempt}).",
                        args.RetryDelay,
                        args.AttemptNumber + 1);

                    return default;
                }
            })
            .Build();
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _insertBackgroundTask = Task.Run(async () => await Insert(), cancellationToken);

        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_insertBackgroundTask != null)
            try
            {
                await _insertBackgroundTask;
            }
            catch (Exception e) when (e is TaskCanceledException or OperationCanceledException)
            {
                // Expected on cancellation
            }
            finally
            {
                _insertBackgroundTask.Dispose();
            }

        _cancellationTokenSource.Dispose();
    }

    public async Task StartingAsync(CancellationToken cancellationToken)
    {
        await _resiliencePipeline.ExecuteAsync(
            static async (configuration, token) =>
            {
                using ClickHouseClient clickHouseClient = new(configuration.ConnectionString);

                var format = string.Format(ClickHouseQueries.CreateTable, configuration.Table);
                await clickHouseClient.ExecuteNonQueryAsync(
                    format, null, null, token);
            },
            _configuration,
            cancellationToken);
    }

    public Task StartedAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }

    public async Task StoppingAsync(CancellationToken cancellationToken)
    {
        await _xEventReader.Completion;
        await _cancellationTokenSource.CancelAsync();
    }

    public Task StoppedAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }

    private async Task Insert()
    {
        try
        {
            await _resiliencePipeline.ExecuteAsync(static async (state, cancellationToken) =>
                {
                    var (xEventReader, logger, config) = state;

                    using ClickHouseClient clickHouseClient = new(config.ConnectionString);

                    await Parallel.ForEachAsync(
                        xEventReader.IntoBatches(config.BatchSize, cancellationToken),
                        new ParallelOptions
                        {
                            MaxDegreeOfParallelism = config.MaxDegreeOfParallelism,
                            CancellationToken = cancellationToken
                        },
                        async (batch, token) =>
                        {
                            Pipe pipe = new(PipeOptions);

                            var producer = Task.Run(async () =>
                            {
                                try
                                {
                                    {
                                        await using var pipeWriterStream = pipe.Writer.AsStream();

                                        await using var targetStream = config.Compression switch
                                        {
                                            ClickHouseCompression.GZip => new GZipStream(
                                                pipeWriterStream,
                                                CompressionMode.Compress,
                                                leaveOpen: true),
                                            _ => pipeWriterStream
                                        };

                                        var encodingWriter = PipeWriter.Create(targetStream);

                                        for (var i = 0; i < batch.Count; i++)
                                        {
                                            var xEvent = batch[i];

                                            if (xEvent is null)
                                                continue;

                                            if (!await ClickHouseXEventEncoder.WriteXEventToPipe(encodingWriter, xEvent,
                                                    token))
                                                break;
                                        }

                                        await encodingWriter.CompleteAsync();

                                        if (targetStream is GZipStream gzip)
                                            await gzip.FlushAsync(token);
                                    }

                                    await pipe.Writer.CompleteAsync();
                                }
                                catch (Exception ex)
                                {
                                    await pipe.Writer.CompleteAsync(ex);
                                }
                                finally
                                {
                                    batch.Dispose();
                                }
                            }, token);

                            await using var finalStream = pipe.Reader.AsStream();

                            try
                            {
                                await clickHouseClient.PostStreamAsync(
                                    string.Format(ClickHouseQueries.Insert, config.Table),
                                    finalStream,
                                    config.Compression is ClickHouseCompression.GZip,
                                    token);
                            }
                            catch (Exception ex)
                            {
                                logger.LogError(ex, "Failed to insert XEvent batch into ClickHouse.");
                            }
                            finally
                            {
                                await producer;
                                await pipe.Reader.CompleteAsync();
                            }
                        });
                },
                (XEventReader: _xEventReader, Logger: _logger, Config: _configuration),
                _cancellationTokenSource.Token);
        }
        catch (Exception e) when (e is TaskCanceledException or OperationCanceledException)
        {
            _logger.LogInformation("ClickHouse XEvent insertion stopped gracefully.");
        }
    }
}