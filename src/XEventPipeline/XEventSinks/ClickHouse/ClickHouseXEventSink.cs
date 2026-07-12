using System.Buffers;
using System.IO.Compression;
using System.IO.Pipelines;
using ClickHouse.Driver;
using ClickHouse.Driver.ADO;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Polly;
using Polly.Retry;
using XEventPipeline.Configurations;
using XEventPipeline.XEventBuffer;

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
    private readonly ILoggerFactory _loggerFactory;
    private readonly ResiliencePipeline _resiliencePipeline;

    private readonly XEventBufferReader _xEventBufferReader;

    private Task? _insertBackgroundTask;

    public ClickHouseXEventSink(
        IOptions<ClickHouseConfiguration> configuration,
        XEventBufferReader xEventBufferReader,
        ILoggerFactory loggerFactory)
    {
        _configuration = configuration.Value;
        _xEventBufferReader = xEventBufferReader;
        _loggerFactory = loggerFactory;
        _logger = loggerFactory.CreateLogger<ClickHouseXEventSink>();

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

                await clickHouseClient.ExecuteNonQueryAsync(
                    string.Format(ClickHouseQueries.CreateTable, configuration.Table),
                    null,
                    null,
                    token);
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
        await _xEventBufferReader.Completion.WaitAsync(cancellationToken);
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
                    var (xEventReader, loggerFactory, logger, config) = state;

                    using ClickHouseClient clickHouseClient = new(new ClickHouseClientSettings(config.ConnectionString)
                    {
                        UseCompression = config.Compression == ClickHouseCompression.GZip,
                        LoggerFactory = loggerFactory
                    });

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
                                                true),
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
                (_xEventBufferReader, _loggerFactory, _logger, _configuration),
                _cancellationTokenSource.Token);
        }
        catch (Exception e) when (e is TaskCanceledException or OperationCanceledException)
        {
            _logger.LogInformation("ClickHouse XEvent insertion stopped gracefully.");
        }
    }
}