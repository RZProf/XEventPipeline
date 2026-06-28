# XEventPipeline

A .NET 10 background service that streams SQL Server Extended Events (XEvents) in real time and forwards them to a configurable sink — ClickHouse, PostgreSQL, or Kafka.

## Motivation

SQL Server Extended Events is the recommended low-overhead tracing mechanism in SQL Server, but getting that data into a modern analytics or observability stack is non-trivial. The older SQL Profiler approach is deprecated and imposes significant server-side overhead. XEvents themselves are lightweight, yet the out-of-the-box tooling only writes to files or ring buffers — there is no built-in way to stream events continuously to an external system.

XEventPipeline fills that gap. It manages the XEvent session for you, reads the live stream with near-zero overhead, and routes every event to a sink you already operate — without requiring any agents, linked servers, or proprietary add-ons.

## Use cases

- **Slow query detection** — capture `sp_statement_completed` or `sql_batch_completed` events filtered by duration and feed them to ClickHouse or PostgreSQL for real-time dashboards and alerting.
- **Query analytics** — aggregate query hashes, execution counts, and durations over time to identify regressions or understand workload patterns.
- **Audit logging** — record `sql_statement_completed`, login events, or DDL changes to an append-only store for compliance and forensics.
- **Security monitoring** — stream failed login attempts or privilege-escalation events into a Kafka topic consumed by a SIEM or alerting pipeline.
- **Capacity planning** — collect long-running statistics on blocking, waits, and I/O to inform index tuning and infrastructure sizing decisions.

## How it works

On startup the service creates (or recreates) an XEvent session on the target SQL Server instance according to the events declared in `appsettings.yml`. It then opens a live stream via `XELiteEventStreamer` and writes each event into a bounded in-memory channel. A sink service reads from that channel and bulk-inserts or produces the events to the chosen destination.

```
SQL Server XEvent session
        │
        ▼
  XEventStreamer (hosted service)
        │  bounded channel (default 100 000 events)
        ▼
  Sink (one of):
    ├── ClickHouseXEventSink  → ClickHouse table
    ├── PostgresXEventSink    → PostgreSQL table
    └── KafkaXEventSink       → Kafka topic
```

All three layers use Polly for automatic retries with exponential back-off so transient connectivity failures self-heal without operator intervention. The XEvent session is also self-healing: if the stream drops, the streamer verifies the session is still alive before reconnecting.

## Prerequisites

- [.NET 10 SDK](https://dotnet.microsoft.com/download)
- A running SQL Server instance with an account that has `ALTER ANY EVENT SESSION` permission
- One of: ClickHouse, PostgreSQL, or a Kafka broker

## Configuration

All configuration lives in `src/XEventPipeline/appsettings.yml`. Provide exactly **one** of the three sink sections (`ClickHouse`, `Postgres`, or `Kafka`); the application will fail fast if zero or more than one are present.

```yaml
Settings:
  BoundedCapacity: 100000       # in-memory channel buffer size

SqlServer:
  ConnectionString: "<connection string>"
  SessionName: xe_pipeline       # optional, defaults to "xe_pipeline"
  Events:
    - Package: sqlserver
      Name: sp_statement_completed
      CustomizableAttributes:
        - Name: collect_statement
          Value: 1
      Actions:
        - client_app_name
        - client_hostname
        - database_name
        - query_hash
        - username
      PredicateExpression: "[duration]>= 5000000"  # optional WHERE clause
    - Package: sqlserver
      Name: sql_batch_completed
      # ...

# --- Pick exactly ONE sink ---

ClickHouse:
  ConnectionString: "<connection string>"
  Table: xe_data                 # optional, defaults to "xe_data"
  BatchSize: 10000
  MaxDegreeOfParallelism: 4
  Compression: None              # None | GZip

Postgres:
  ConnectionString: "<connection string>"
  Table: xe_data                 # optional, defaults to "xe_data"
  BatchSize: 10000
  MaxDegreeOfParallelism: 4

Kafka:
  BrokerAddress: "<broker:port>"
  Topic: "<topic>"
  CompressionType: None          # None | Gzip | Snappy | Lz4 | Zstd
  LingerMs: 5
  BatchSize: 10000
  MaxDegreeOfParallelism: 10
  ProduceTimeout: 1000           # ms
  DateTimeFormatString:          # optional custom format for DateTime fields
```

## Running

```bash
# from the repository root
dotnet run --project src/XEventPipeline
```

## Building

```bash
dotnet build
```

Self-contained single-file publish (example for Linux):

```bash
dotnet publish src/XEventPipeline -c Release -r linux-x64 \
  --self-contained true \
  /p:PublishSingleFile=true \
  /p:PublishReadyToRun=true
```

Pre-built binaries for Windows (x64), Linux (x64), and macOS (ARM64) are attached to each [GitHub Release](../../releases).

## Running the integration tests

The integration tests in `test/XEventPipeline.IntegrationTests` require live instances of SQL Server and whichever sink(s) you want to exercise. Update the connection strings in the test project's `appsettings.yml`, then:

```bash
dotnet test test/XEventPipeline.IntegrationTests
```

## Project structure

```
src/
  XEventPipeline/
    Program.cs                        # host setup & sink selection
    XEventStreamer.cs                  # reads from SQL Server, writes to channel
    XEventSessionManager.cs           # creates/drops the XEvent session
    XEventSessionQueries.cs           # DDL query builders
    ChannelExtensions.cs              # batching helpers for ChannelReader
    Configurations/                   # strongly-typed config classes
    XEventSinks/
      ClickHouse/                     # ClickHouse sink + encoder + queries
      Postgres/                       # PostgreSQL sink + encoder + queries
      Kafka/                          # Kafka producer sink
test/
  XEventPipeline.IntegrationTests/   # end-to-end tests against real services
```

## Key dependencies

| Package | Purpose |
|---|---|
| `Microsoft.SqlServer.XEvent.XELite` | Live XEvent stream reader |
| `Microsoft.Data.SqlClient` | SQL Server connectivity |
| `ClickHouse.Driver` | ClickHouse client |
| `Npgsql` | PostgreSQL client |
| `Confluent.Kafka` | Kafka producer |
| `Polly.Core` | Resilience & retry pipelines |
| `SpanJson` | High-performance JSON serialisation (Kafka sink) |
| `NetEscapades.Configuration.Yaml` | YAML configuration provider |
