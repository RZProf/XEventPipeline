# XEventPipeline: SQL Server Extended Events to Kafka, ClickHouse, and PostgreSQL

[![Continuous Integration](https://github.com/RZProf/XEventPipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/RZProf/XEventPipeline/actions/workflows/ci.yml)
[![Latest release](https://img.shields.io/github/v/release/RZProf/XEventPipeline)](https://github.com/RZProf/XEventPipeline/releases/latest)

**XEventPipeline is a .NET 10 SQL Server Extended Events (XEvents/XE) streaming service.** It creates and manages a SQL Server event session, reads events as they occur, and sends them to exactly one destination: ClickHouse, PostgreSQL, or Apache Kafka.

Configure the pipeline with YAML or use its built-in Blazor setup page to browse the SQL Server event catalog, choose events and actions, set predicates, enter connection details, and save `appsettings.yml`. The setup UI lives in a Razor class library and runs inside the executable’s minimal web host only in configure mode; normal startup remains a console worker.

## Features

- Stream SQL Server Extended Events in real time with [XELite](https://github.com/microsoft/sql-server-xevent)
- Select multiple event types, event-specific settings, actions, and predicate expressions
- Send each event’s metadata, actions, and data fields to ClickHouse, PostgreSQL, or Kafka
- Create destination tables automatically for ClickHouse and PostgreSQL
- Buffer and batch events in memory, with retry and reconnect handling
- Export traces, metrics, and logs over OpenTelemetry Protocol (OTLP)
- Configure the pipeline in a browser or edit YAML directly

Common use cases include SQL Server query performance monitoring, slow statement analysis, workload analytics, audit logging, and routing database events to Kafka-based observability or security systems.

## How it works

At startup, XEventPipeline creates or replaces its configured SQL Server Extended Events session and starts streaming events. A bounded in-memory channel passes events to one configured sink, which batches and writes them to the destination. If SQL Server or the sink becomes temporarily unavailable, the services retry with backoff.

The event session uses SQL Server’s `ALLOW_SINGLE_EVENT_LOSS` retention mode. This permits SQL Server to drop an event under pressure instead of blocking the workload; this pipeline does not guarantee lossless capture.

```text
SQL Server Extended Events
          │
          ▼
 Live stream → bounded buffer → one configured sink
                              ├── ClickHouse table
                              ├── PostgreSQL table
                              └── Kafka topic
```

ClickHouse and PostgreSQL store event metadata, actions, and fields in columns, with action and field values stored as JSON. Kafka receives a JSON event containing `UUID`, `Name`, `Timestamp`, XEvent offsets and size, `Actions`, and `Fields`.

## Requirements and SQL Server permissions

- .NET 10 SDK to build or run from source
- A SQL Server instance reachable from the machine running XEventPipeline
- One destination: ClickHouse, PostgreSQL, or Kafka
- SQL credentials for the SQL Server connection

The SQL Server login needs permission to read the Extended Events catalog and create, alter, and drop the server event session. For SQL Server 2019 and earlier, catalog DMV access requires `VIEW SERVER STATE`. For SQL Server 2022 and later, it requires `VIEW SERVER PERFORMANCE STATE`. Session permissions vary by SQL Server version: older versions use `ALTER ANY EVENT SESSION`; SQL Server 2022 and later support the more granular `CREATE ANY EVENT SESSION`, `ALTER ANY EVENT SESSION`, and `DROP ANY EVENT SESSION` permissions. See Microsoft’s documentation for [CREATE EVENT SESSION](https://learn.microsoft.com/en-us/sql/t-sql/statements/create-event-session-transact-sql), [ALTER EVENT SESSION](https://learn.microsoft.com/en-us/sql/t-sql/statements/alter-event-session-transact-sql), and [DROP EVENT SESSION](https://learn.microsoft.com/en-us/sql/t-sql/statements/drop-event-session-transact-sql).

XEventPipeline creates a server-scoped session named `xe_pipeline` by default. It stops and drops that session when the host stops, and replaces an existing session with the same name at startup. Use a dedicated session name if you manage other Extended Events sessions on the server.

## Quick start

Clone the repository and start the Blazor configuration page:

```bash
git clone https://github.com/RZProf/XEventPipeline.git
cd XEventPipeline
dotnet run --project src/XEventPipeline -- --configure
```

Configure mode opens a browser tab. The page reads the existing `src/XEventPipeline/appsettings.yml` and pre-fills its SQL Server and sink settings. If the file is missing, setup starts with empty values; if it cannot be read, setup starts with empty values and shows a warning. Saving creates or replaces the file. Enter or update the SQL Server connection details, then choose **Connect and load events**. Search the catalog, select one or more event types, and configure predicates, customizable settings, and actions. Choose one sink, set the event buffer capacity (the maximum number of events held in memory while waiting for the sink), and save.

The event and action pickers search large SQL Server catalogs and display results in batches. When saving succeeds, the page shows the updated file path and the next step. Use **Edit again** to return to the form. When the last configure tab disconnects, the host waits five seconds for a reconnection before stopping; this gives a refreshed page time to reconnect. Close the tab after saving, then start the application without the configure flag:

```bash
dotnet run --project src/XEventPipeline
```

The setup page uses SQL Server’s catalog to show each event’s fields. SQL Server emits an event’s defined data fields automatically; event fields cannot be individually enabled or disabled in an Extended Events session definition. Use event predicates to filter captured events and actions to add action values.

## YAML configuration

The application reads `src/XEventPipeline/appsettings.yml` by default. Configure SQL Server and one sink section only. The following example is valid YAML for two events and a ClickHouse sink:

```yaml
Settings:
  BoundedCapacity: 100000

SqlServer:
  ConnectionString: "Server=tcp:sql.example.net,1433;Database=master;User ID=xe_reader;Password=change-me;Encrypt=True;TrustServerCertificate=True"
  SessionName: xe_pipeline
  Events:
    - Name: sqlserver.sp_statement_completed
      CustomizableAttributes:
        - Name: collect_statement
          Value: "1"
      Actions:
        - sqlserver.client_app_name
        - sqlserver.client_hostname
        - sqlserver.database_name
        - sqlserver.query_hash
        - sqlserver.username
      PredicateExpression: "[duration] >= 5000000"
    - Name: sqlserver.sql_batch_completed
      Actions:
        - sqlserver.client_app_name
      PredicateExpression: "[duration] >= 5000000"

# Configure exactly one sink section.
ClickHouse:
  ConnectionString: "Host=clickhouse.example.net;Port=8123;Database=default;Username=default;Password=change-me;Protocol=http"
  Table: xe_data
  BatchSize: 10000
  MaxDegreeOfParallelism: 4
  Compression: None  # None or GZip
```

The SQL Server connection string should use the format supported by [`Microsoft.Data.SqlClient`](https://learn.microsoft.com/en-us/sql/connect/ado-net/connection-string-syntax). The setup page builds it from the server, port, database, SQL username, password, and certificate setting.

### Sink options

Choose one of these top-level sections:

<details>
<summary>ClickHouse</summary>

```yaml
ClickHouse:
  ConnectionString: "Host=clickhouse.example.net;Port=8123;Database=default;Username=default;Password=change-me;Protocol=http"
  Table: xe_data
  BatchSize: 10000
  MaxDegreeOfParallelism: 4
  Compression: None  # None or GZip
```

XEventPipeline creates the table if it does not already exist. The configured database and table must be accessible to the ClickHouse user.
</details>

<details>
<summary>PostgreSQL</summary>

```yaml
Postgres:
  ConnectionString: "Host=postgres.example.net;Port=5432;Database=xevents;Username=xe_writer;Password=change-me;SSL Mode=Prefer"
  Table: xe_data
  BatchSize: 10000
  MaxDegreeOfParallelism: 4
```

XEventPipeline creates the partitioned table if needed and maintains daily partitions. The PostgreSQL user needs permission to create and write to the configured table.
</details>

<details>
<summary>Kafka</summary>

```yaml
Kafka:
  BrokerAddress: "kafka.example.net:9092"
  Topic: sqlserver-xevents
  CompressionType: None  # None, Gzip, Snappy, Lz4, or Zstd
  LingerMs: 5
  BatchSize: 10000
  MaxDegreeOfParallelism: 10
  ProduceTimeout: 1000
```

</details>

The setup page writes the selected sink’s connection string and options to the same YAML file used by normal application startup. Treat the file as sensitive: it can contain SQL Server and sink passwords. For deployments, protect its filesystem permissions and consider your environment’s supported secret-management approach.

## OpenTelemetry

The service exports traces, metrics, and logs using the OpenTelemetry Protocol (OTLP). Configure the exporter with the standard OpenTelemetry environment variables, such as `OTEL_EXPORTER_OTLP_ENDPOINT`. The application also reports the `xeventpipeline.queue.size` metric for events waiting in the in-memory buffer.

## Build and run

Build the solution:

```bash
dotnet build
```

Run the pipeline using the YAML file:

```bash
dotnet run --project src/XEventPipeline
```

Run only the configuration UI:

```bash
dotnet run --project src/XEventPipeline -- --configure
```

Publish a self-contained app for a specific runtime identifier (example: Linux x64):

```bash
dotnet publish src/XEventPipeline/XEventPipeline.csproj \
  --configuration Release \
  --runtime linux-x64 \
  --self-contained true
```

Prebuilt release artifacts are available for Windows x64, Linux x64, and macOS x64/ARM64 on the [GitHub Releases page](https://github.com/RZProf/XEventPipeline/releases).

## Integration tests

The integration tests use Testcontainers to start SQL Server and the destination service in Docker. Docker must be installed and running. From the repository root, run:

```bash
dotnet test --project test/XEventPipeline.IntegrationTests/XEventPipeline.IntegrationTests.csproj --configuration Release
```

The tests exercise SQL Server streaming and each supported sink, so they may take several minutes to complete.

## Project structure

```text
src/XEventPipeline/
  Configurations/             YAML-bound application settings
  XEventBuffer/               Bounded channel and batch reader/writer
  XEventSinks/                ClickHouse, PostgreSQL, and Kafka sinks
  Program.cs                  Console host and --configure minimal web host
  XEventSessionManager.cs     SQL Server session lifecycle
  XEventSessionQueries.cs     Extended Events session SQL
  XEventStreamer.cs           Live SQL Server event stream
src/XEventPipeline.SetupUi/
  Components/                Blazor setup page and event/action pickers
  Setup/                      YAML file handling, connection strings, XE catalog
test/XEventPipeline.IntegrationTests/
                              Docker-backed end-to-end tests
```
