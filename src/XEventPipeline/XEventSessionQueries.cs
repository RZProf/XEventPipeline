using XEventPipeline.Configurations;

namespace XEventPipeline;

public static class XEventSessionQueries
{
    private const string CreateSession = """
                                         DECLARE @sql NVARCHAR(MAX);
                                         DECLARE @events NVARCHAR(MAX);

                                         IF EXISTS (SELECT 1 FROM sys.server_event_sessions WHERE name = @sessionName)
                                           BEGIN
                                               SET @sql = N'ALTER EVENT SESSION ' + @sessionName + N' ON SERVER STATE = STOP';
                                               EXEC sp_executesql @sql;
                                               
                                               SET @sql = N'DROP EVENT SESSION ' + @sessionName + N' ON SERVER';
                                               EXEC sp_executesql @sql;
                                           END
                                         SET @events =
                                             {0}

                                         SET @sql = N'CREATE EVENT SESSION ' + @sessionName + N' ON SERVER ' +
                                                    @events +
                                                    N' WITH (
                                                         MAX_MEMORY=4096 KB,
                                                         EVENT_RETENTION_MODE=ALLOW_SINGLE_EVENT_LOSS,
                                                         MAX_DISPATCH_LATENCY=30 SECONDS,
                                                         MAX_EVENT_SIZE=0 KB,
                                                         MEMORY_PARTITION_MODE=NONE,
                                                         TRACK_CAUSALITY=ON,
                                                         STARTUP_STATE=OFF)';

                                         EXEC sp_executesql @sql;

                                         SET @sql = N'ALTER EVENT SESSION ' + @sessionName + N' ON SERVER STATE = START';
                                         EXEC sp_executesql @sql;
                                         """;

    private const string DropSession = """
                                       DECLARE @sql NVARCHAR(MAX);

                                       IF EXISTS (SELECT 1 FROM sys.server_event_sessions WHERE name = @sessionName)
                                         BEGIN
                                             SET @sql = N'ALTER EVENT SESSION ' + @sessionName + N' ON SERVER STATE = STOP';
                                             EXEC sp_executesql @sql;
                                             
                                             SET @sql = N'DROP EVENT SESSION ' + @sessionName + N' ON SERVER';
                                             EXEC sp_executesql @sql;
                                         END
                                       """;

    private const string SessionIsRunning = """
                                            DECLARE @sql NVARCHAR(MAX);
                                            DECLARE @events NVARCHAR(MAX);

                                            IF EXISTS (SELECT 1 FROM sys.server_event_sessions WHERE name = @sessionName)
                                                BEGIN
                                                    IF NOT EXISTS (SELECT 1 FROM sys.dm_xe_sessions WHERE name = @sessionName)
                                                    BEGIN
                                                        SET @sql = N'ALTER EVENT SESSION ' + @sessionName + N' ON SERVER STATE = START';
                                                        EXEC sp_executesql @sql;
                                                    END
                                                END
                                            ELSE
                                                BEGIN
                                                    SET @events =
                                                            {0}
                                                                          
                                                    SET @sql = N'CREATE EVENT SESSION ' + @sessionName + N' ON SERVER ' +
                                                               @events +
                                                               N' WITH (
                                                                    MAX_MEMORY=4096 KB,
                                                                    EVENT_RETENTION_MODE=ALLOW_SINGLE_EVENT_LOSS,
                                                                    MAX_DISPATCH_LATENCY=30 SECONDS,
                                                                    MAX_EVENT_SIZE=0 KB,
                                                                    MEMORY_PARTITION_MODE=NONE,
                                                                    TRACK_CAUSALITY=ON,
                                                                    STARTUP_STATE=OFF)';
                                                    
                                                    EXEC sp_executesql @sql;
                                                    
                                                    SET @sql = N'ALTER EVENT SESSION ' + @sessionName + N' ON SERVER STATE = START';
                                                    EXEC sp_executesql @sql;
                                                END
                                            """;

    private const string SessionExists = """
                                         SELECT 
                                            CASE 
                                                WHEN EXISTS (SELECT 1 FROM sys.server_event_sessions WHERE name = @sessionName)
                                                    AND EXISTS (SELECT 1 FROM sys.dm_xe_sessions WHERE name = @sessionName)
                                                THEN 1
                                                ELSE 0
                                            END
                                         """;

    public static string InitializationQuery(XEventConfiguration[] xEventConfigurations)
    {
        var events = string.Join("""
                                 + ',' +
                                            
                                 """, xEventConfigurations.Select(e => $"N'ADD {e}' "));

        return string.Format(CreateSession, events);
    }

    public static string DropQuery()
    {
        return DropSession;
    }

    public static string SessionIsRunningQuery(XEventConfiguration[] xEventConfigurations)
    {
        var events = string.Join("""
                                 + ',' +
                                            
                                 """, xEventConfigurations.Select(e => $"N'ADD {e}' "));

        return string.Format(SessionIsRunning, events);
    }
}