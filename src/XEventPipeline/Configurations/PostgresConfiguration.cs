namespace XEventPipeline.Configurations;

public class PostgresConfiguration
{
    private const string DefaultTableName = "xe_data";

    public required string ConnectionString { get; set; }

    public string Table
    {
        get => field ?? DefaultTableName;
        set;
    }

    public int BatchSize { get; set; } = 100_000;

    public int MaxDegreeOfParallelism { get; init; } = 4;
}