namespace XEventPipeline.XEventSinks.ClickHouse;

public static class ClickHouseQueries
{
    public const string CreateTable = """
                                      CREATE TABLE IF NOT EXISTS {0}
                                      (
                                          `UUID` UUID,
                                          Name String,
                                          Timestamp DateTime,
                                          XEventStartOffsetInBytes  Int64,
                                          XEventEndOffsetInBytes Int64,
                                          XEventSizeInBytes Int64,
                                          Actions JSON,
                                          Fields JSON
                                      )
                                      ENGINE = MergeTree()
                                      ORDER BY (`UUID`)
                                      """;

    public const string Insert = """
                                 INSERT INTO {0} 
                                 (`UUID`, Name, Timestamp, XEventStartOffsetInBytes, XEventEndOffsetInBytes, XEventSizeInBytes, Actions, Fields)
                                 FORMAT RowBinary
                                 """;
}