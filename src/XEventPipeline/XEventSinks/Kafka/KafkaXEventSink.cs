using System.Buffers;
using System.Threading.Channels;
using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.SqlServer.XEvent.XELite;
using Polly;
using Polly.Retry;
using SpanJson;
using XEventPipeline.Configurations;

namespace XEventPipeline.XEventSinks.Kafka;

public class KafkaXEventSink : IHostedService
{
    private readonly CancellationTokenSource _cancellationTokenSource;
    private readonly ILogger<KafkaXEventSink> _logger;
    private readonly Task _produceBackgroundTask;
    private readonly ChannelReader<IXEvent> _xEventReader;
    private readonly IProducer<Null, ArraySegment<byte>> _producer;

    public KafkaXEventSink(
        IOptions<KafkaConfiguration> configuration,
        ChannelReader<IXEvent> xEventReader,
        ILogger<KafkaXEventSink> logger)
    {
        _xEventReader = xEventReader;
        _logger = logger;

        _cancellationTokenSource = new CancellationTokenSource();

        var resiliencePipeline = new ResiliencePipelineBuilder()
            .AddRetry(new RetryStrategyOptions
            {
                ShouldHandle =
                    new PredicateBuilder().Handle<KafkaException>(ex => ex.Error.Code == ErrorCode.Local_QueueFull),
                MaxRetryAttempts = int.MaxValue,
                BackoffType = DelayBackoffType.Constant,
                UseJitter = true,
                Delay = TimeSpan.FromMilliseconds(50),
                OnRetry = args =>
                {
                    _logger.LogError(
                        args.Outcome.Exception,
                        "Failed to produce XEvents into Kafka. Retrying in {Delay:g} (attempt #{Attempt}).",
                        args.RetryDelay,
                        args.AttemptNumber + 1);

                    return default;
                }
            })
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
                        "Failed to produce XEvents into Kafka. Retrying in {Delay:g} (attempt #{Attempt}).",
                        args.RetryDelay,
                        args.AttemptNumber + 1);

                    return default;
                }
            })
            .Build();

        _producer = new ProducerBuilder<Null, ArraySegment<byte>>(configuration.Value.ToProducerConfig())
            .SetLogHandler((_, l) =>
            {
                switch (l.Level)
                {
                    case SyslogLevel.Emergency:
                    case SyslogLevel.Alert:
                    case SyslogLevel.Critical:
                        logger.LogCritical("[Kafka-{Name}][{Facility}] {Message}", l.Name, l.Facility, l.Message);
                        break;
                    case SyslogLevel.Error:
                        logger.LogError("[Kafka-{Name}][{Facility}] {Message}", l.Name, l.Facility, l.Message);
                        break;
                    case SyslogLevel.Warning:
                        logger.LogWarning("[Kafka-{Name}][{Facility}] {Message}", l.Name, l.Facility, l.Message);
                        break;
                    case SyslogLevel.Notice:
                    case SyslogLevel.Info:
                        if (logger.IsEnabled(LogLevel.Information))
                            logger.LogInformation("[Kafka-{Name}][{Facility}] {Message}", l.Name, l.Facility,
                                l.Message);
                        break;
                    case SyslogLevel.Debug:
                        if (logger.IsEnabled(LogLevel.Debug))
                            logger.LogDebug("[Kafka-{Name}][{Facility}] {Message}", l.Name, l.Facility, l.Message);
                        break;
                    default:
                        if (logger.IsEnabled(LogLevel.Information))
                            logger.LogInformation("[Kafka-{Name}][{Facility}] {Message}", l.Name, l.Facility,
                                l.Message);
                        break;
                }
            })
            .SetErrorHandler((p, e) =>
            {
                logger.LogError(
                    "Kafka Producer Error - Name: {Name}, Code: {Code}, Reason: {Reason}, IsFatal: {IsFatal}",
                    p.Name, e.Code, e.Reason, e.IsFatal);
            })
            .SetStatisticsHandler((p, json) =>
            {
                // Better yet: Pass 'json' to a metrics library, not a text logger!
                if (logger.IsEnabled(LogLevel.Debug))
                    logger.LogDebug("Kafka Producer Stats - Name: {Name}, Data: {Json}", p.Name, json);
            })
            .SetValueSerializer(new ArraySegmentSerializer())
            .Build();

        _produceBackgroundTask = Parallel.ForEachAsync(_xEventReader.ReadAllAsync(_cancellationTokenSource.Token),
            new ParallelOptions
            {
                CancellationToken = _cancellationTokenSource.Token,
                MaxDegreeOfParallelism = configuration.Value.MaxDegreeOfParallelism
            }, (xEvent, token) => resiliencePipeline.ExecuteAsync(static (state, _) =>
            {
                var (producer, extendedEvent, logger, topic) = state;
                var payload =
                    JsonSerializer.Generic.Utf8.SerializeToArrayPool<IXEvent, XEventResolver<byte>>(extendedEvent);

                if (payload.Array is null)
                    return default;

                try
                {
                    producer!.Produce(topic, new Message<Null, ArraySegment<byte>> { Value = payload });

                    if (logger.IsEnabled(LogLevel.Trace))
                        logger.LogTrace("Produced successfully");
                }
                finally
                {
                    if (payload.Array is not null)
                        ArrayPool<byte>.Shared.Return(payload.Array);
                }

                return default;
            }, (_producer, xEvent, _logger, configuration.Value.Topic), token));
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        try
        {
            await _xEventReader.Completion.WaitAsync(cancellationToken);
            _producer.Flush(cancellationToken);
            await _produceBackgroundTask.WaitAsync(cancellationToken);
            await _cancellationTokenSource.CancelAsync();
        }
        catch (OperationCanceledException)
        {
            // Expected
        }
        catch (Exception e)
        {
            _logger.LogError(e, "Unexpected error while buffer completion!");
        }
    }
}