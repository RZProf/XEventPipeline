using System.Buffers;
using System.Buffers.Binary;
using System.Data;
using System.IO.Pipelines;
using System.Threading.Channels;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.SqlServer.XEvent.XELite;
using Npgsql;
using Polly;
using Polly.Retry;
using XEventPipeline.Configurations;

namespace XEventPipeline.XEventSinks.Postgres;

public class PostgresXEventSink : IHostedLifecycleService
{
    private static readonly PipeOptions PipeOptions = new(
        MemoryPool<byte>.Shared,
        pauseWriterThreshold: 1024 * 1024 * 4,
        resumeWriterThreshold: 1024 * 1024 * 2,
        useSynchronizationContext: false);

    private static readonly byte[] PgHeader =
    [
        0x50, 0x47, 0x43, 0x4F, 0x50, 0x59, 0x0A, 0xFF, 0x0D, 0x0A, 0x00,
        0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00
    ];

    private readonly CancellationTokenSource _cancellationTokenSource;
    private readonly PostgresConfiguration _configuration;
    private readonly ILogger<PostgresXEventSink> _logger;
    private readonly ResiliencePipeline _resiliencePipeline;
    private readonly ChannelReader<IXEvent> _xEventReader;

    private Task? _createPartitionsBackgroundTask;
    private Task? _copyBackgroundTask;

    public PostgresXEventSink(
        IOptions<PostgresConfiguration> configuration,
        ChannelReader<IXEvent> xEventReader,
        ILogger<PostgresXEventSink> logger)
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
                        "Failed to insert XEvents into Postgres. Retrying in {Delay:g} (attempt #{Attempt}).",
                        args.RetryDelay,
                        args.AttemptNumber + 1);

                    return default;
                }
            })
            .Build();
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _createPartitionsBackgroundTask = Task.Run(async () => await CreatePartitions(), cancellationToken);
        _copyBackgroundTask = Task.Run(async () => await Copy(), cancellationToken);

        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        try
        {
            await Task.WhenAll(_createPartitionsBackgroundTask ?? Task.CompletedTask,
                _copyBackgroundTask ?? Task.CompletedTask);
        }
        catch (Exception e) when (e is TaskCanceledException or OperationCanceledException)
        {
            // Expected on cancellation
        }
        finally
        {
            _createPartitionsBackgroundTask?.Dispose();
            _copyBackgroundTask?.Dispose();
        }

        _cancellationTokenSource.Dispose();
    }

    public async Task StartingAsync(CancellationToken cancellationToken)
    {
        await _resiliencePipeline.ExecuteAsync(
            static async (configuration, token) =>
            {
                await using var npgsqlConnection = new NpgsqlConnection(configuration.ConnectionString);
                await npgsqlConnection.OpenAsync(token);

                var npgsqlCmd = npgsqlConnection.CreateCommand();

                npgsqlCmd.CommandText = string.Format(PostgresQueries.CreateTable, configuration.Table);
                npgsqlCmd.CommandType = CommandType.Text;

                await npgsqlCmd.ExecuteNonQueryAsync(token);
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

    private async Task CreatePartitions()
    {
        using var timer = new PeriodicTimer(TimeSpan.FromDays(1));

        try
        {
            do
            {
                await _resiliencePipeline.ExecuteAsync(
                    static async (state, cancellationToken) =>
                    {
                        var (logger, config) = state;

                        await using var npgsqlConnection = new NpgsqlConnection(config.ConnectionString);
                        await npgsqlConnection.OpenAsync(cancellationToken);
    
                        using var npgsqlCmd = npgsqlConnection.CreateCommand();
                        npgsqlCmd.CommandType = CommandType.Text;

                        var today = DateTime.UtcNow;
                        npgsqlCmd.CommandText = PostgresQueries.CreatePartition(config.Table, today);
                        await npgsqlCmd.ExecuteNonQueryAsync(cancellationToken);
                        logger.LogInformation("Partition verified/created for today: {Date}", today.ToString("yyyy-MM-dd"));

                        var tomorrow = today.AddDays(1);
                        npgsqlCmd.CommandText = PostgresQueries.CreatePartition(config.Table, tomorrow);
                        await npgsqlCmd.ExecuteNonQueryAsync(cancellationToken);
                        logger.LogInformation("Partition verified/created for tomorrow: {Date}", tomorrow.ToString("yyyy-MM-dd"));
                    },
                    (_logger, _configuration),
                    _cancellationTokenSource.Token);
                
            } while (await timer.WaitForNextTickAsync(_cancellationTokenSource.Token));
        }
        catch (Exception e) when (e is TaskCanceledException or OperationCanceledException)
        {
            _logger.LogInformation("Postgres partition creation stopped gracefully.");
        }
    }

    private async Task Copy()
    {
        try
        {
            await _resiliencePipeline.ExecuteAsync(
                static async (state, cancellationToken) =>
                {
                    var (xEventReader, logger, config) = state;

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
                                    pipe.Writer.Write(PgHeader);

                                    for (var index = 0; index < batch.Count; index++)
                                    {
                                        var xEvent = batch[index];

                                        if (xEvent is null)
                                            continue;

                                        if (!await PostgresXEventEncoder.WriteXEventToPipe(pipe.Writer, xEvent, token))
                                            break;
                                    }

                                    var trailerMemory = pipe.Writer.GetMemory(2);
                                    BinaryPrimitives.WriteInt16BigEndian(trailerMemory.Span, -1);
                                    pipe.Writer.Advance(2);

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

                            var consumer = Task.Run(async () =>
                            {
                                await using var npgsqlConnection = new NpgsqlConnection(config.ConnectionString);
                                await npgsqlConnection.OpenAsync(token);

                                await using var dbStream = await npgsqlConnection.BeginRawBinaryCopyAsync(
                                        string.Format(PostgresQueries.Copy, config.Table),
                                        token);

                                await pipe.Reader.CopyToAsync(dbStream, token);
                            }, token);

                            try
                            {
                                await Task.WhenAll(producer, consumer);
                            }
                            catch (Exception ex)
                            {
                                logger.LogError(ex, "Failed to insert XEvent batch into Postgres.");
                            }
                        });
                },
                (_xEventReader, _logger, _configuration),
                _cancellationTokenSource.Token);
        }
        catch (Exception e) when (e is TaskCanceledException or OperationCanceledException)
        {
            _logger.LogInformation("Postgres XEvent insertion stopped gracefully.");
        }
    }
}