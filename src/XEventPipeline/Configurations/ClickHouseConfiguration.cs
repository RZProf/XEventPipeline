namespace XEventPipeline.Configurations;

public class ClickHouseConfiguration
{
    private const string DefaultTableName = "xe_data";

    public required string ConnectionString { get; set; }

    public string Table
    {
        get => field ?? DefaultTableName;
        set;
    }

    public ClickHouseCompression Compression { get; init; }

    public int BatchSize { get; set; } = 100_000;

    public int MaxDegreeOfParallelism { get; init; } = 4;
}

public enum ClickHouseCompression
{
    None = 0,
    GZip = 1
}