using Confluent.Kafka;

namespace XEventPipeline.Configurations;

public class KafkaConfiguration
{
    public string BrokerAddress
    {
        get => field ??
               throw new InvalidOperationException("Unable to configure kafka without broker address.");
        init
        {
            if (string.IsNullOrWhiteSpace(value))
                throw new InvalidOperationException("Unable to configure kafka without broker address.");

            field = value;
        }
    }

    public string Topic
    {
        get => field ?? throw new InvalidOperationException("Unable to configure kafka without topic.");
        init
        {
            if (string.IsNullOrWhiteSpace(value))
                throw new InvalidOperationException("Unable to configure kafka without topic.");

            field = value;
        }
    }

    public string CompressionType
    {
        get => string.IsNullOrWhiteSpace(field) ? "None" : field;
        init
        {
            if (string.IsNullOrWhiteSpace(value))
                value = "None";

            if (!Enum.IsDefined(typeof(CompressionType), value))
                throw new InvalidOperationException(
                    "Unable to configure kafka with invalid compressionType.");

            field = value;
        }
    }

    public int ProduceTimeout { get; init; } = 1000;

    public string? DateTimeFormatString { get; init; }

    public double LingerMs { get; init; } = 0.5;

    public int BatchSize { get; init; } = 1_000_000;

    public int MaxDegreeOfParallelism { get; init; } = 10;

    public ProducerConfig ToProducerConfig()
    {
        return new ProducerConfig
        {
            BootstrapServers = BrokerAddress,
            MessageTimeoutMs = ProduceTimeout,
            LingerMs = LingerMs,
            BatchSize = BatchSize,
            CompressionType = Enum.Parse<CompressionType>(CompressionType)
        };
    }
}