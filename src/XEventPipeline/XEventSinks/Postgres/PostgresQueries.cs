namespace XEventPipeline.XEventSinks.Postgres;

public static class PostgresQueries
{
    public const string CreateTable = """
                                      CREATE TABLE IF NOT EXISTS {0} (
                                          Id BIGINT GENERATED ALWAYS AS IDENTITY,
                                          UUID UUID NOT NULL,
                                          Name VARCHAR(255) NOT NULL, 
                                          Timestamp TIMESTAMPTZ NOT NULL,
                                          XEventStartOffsetInBytes BIGINT,
                                          XEventEndOffsetInBytes BIGINT,
                                          XEventSizeInBytes BIGINT,
                                          Actions JSONB,
                                          Fields JSONB,
                                          PRIMARY KEY (Id, Timestamp)
                                      ) PARTITION BY RANGE (Timestamp);
                                      """;

    public const string Copy =
        """
        COPY {0} (UUID, Name, Timestamp, XEventStartOffsetInBytes, XEventEndOffsetInBytes, XEventSizeInBytes, Actions, Fields)
        FROM STDIN (FORMAT BINARY)
        """;

    public static string CreatePartition(string table, DateTime timestamp)
    {
        var ((year, month, day), _) = timestamp;

        return $"""
                CREATE TABLE IF NOT EXISTS {table}_y{year}m{month}d{day} PARTITION OF {table}
                FOR VALUES FROM ('{year}-{month}-{day} 00:00:00+00') TO ('{year}-{month}-{day + 1} 00:00:00+00');
                CREATE TABLE IF NOT EXISTS {table}_y{year}m{month}d{day + 1} PARTITION OF {table}
                FOR VALUES FROM ('{year}-{month}-{day + 1} 00:00:00+00') TO ('{year}-{month}-{day + 2} 00:00:00+00');
                """;
    }
}