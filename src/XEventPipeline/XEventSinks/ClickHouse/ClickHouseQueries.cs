namespace XEventPipeline.XEventSinks.ClickHouse;

public static class ClickHouseQueries
{
    public const string CreateTable = """
                                      CREATE TABLE IF NOT EXISTS {0} 
                                      (
                                          `UUID` UUID,
                                          Name LowCardinality(String),
                                          `Date` Date DEFAULT toDate(Timestamp),
                                          Timestamp DateTime64(3, 'UTC'),
                                          XEventStartOffsetInBytes Int64,
                                          XEventEndOffsetInBytes Int64,
                                          XEventSizeInBytes Int64,
                                          Actions JSON,
                                          Fields JSON
                                      )
                                      ENGINE = MergeTree()
                                      PARTITION BY toYYYYMM(`Date`)
                                      ORDER BY (`Date`, `UUID`, Timestamp);
                                      """;

    public const string Insert = """
                                 INSERT INTO {0} 
                                 (`UUID`, Name, Timestamp, XEventStartOffsetInBytes, XEventEndOffsetInBytes, XEventSizeInBytes, Actions, Fields)
                                 FORMAT RowBinary
                                 """;
}