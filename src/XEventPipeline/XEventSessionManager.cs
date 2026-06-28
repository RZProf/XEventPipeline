using System.Data;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Polly;
using Polly.Retry;
using XEventPipeline.Configurations;

namespace XEventPipeline;

public class XEventSessionManager : IXEventSessionManager
{
    private readonly SqlServerConfiguration _configuration;
    private readonly ILogger<XEventSessionManager> _logger;
    private readonly ResiliencePipeline _resiliencePipeline;

    public XEventSessionManager(
        IOptions<SqlServerConfiguration> options,
        ILogger<XEventSessionManager> logger)
    {
        _logger = logger;
        _configuration = options.Value;

        _resiliencePipeline = new ResiliencePipelineBuilder()
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
                        "Failed to connect to SQL Server or initialize XEvent session. Retrying in {Delay:g} (attempt #{Attempt}).",
                        args.RetryDelay,
                        args.AttemptNumber + 1);
                    return default;
                }
            })
            .Build();
    }

    public async Task InitializeSession(CancellationToken cancellationToken)
    {
        await _resiliencePipeline.ExecuteAsync(static async (configuration, token) =>
            {
                await using var connection = new SqlConnection(configuration.ConnectionString);
                await connection.OpenAsync(token);
                await using var cmd = connection.CreateCommand();

                cmd.CommandText = XEventSessionQueries.InitializationQuery(configuration.Events);
                cmd.CommandType = CommandType.Text;
                cmd.Parameters.Add(new SqlParameter("@sessionName", SqlDbType.NVarChar)
                {
                    Value = configuration.SessionName
                });

                await cmd.ExecuteNonQueryAsync(token);
            },
            _configuration,
            cancellationToken);
    }

    public async Task MakeSureSessionIsAlive(CancellationToken cancellationToken)
    {
        await _resiliencePipeline.ExecuteAsync(static async (configuration, token) =>
            {
                await using var connection = new SqlConnection(configuration.ConnectionString);
                await connection.OpenAsync(token);
                await using var cmd = connection.CreateCommand();

                cmd.CommandText = XEventSessionQueries.SessionIsRunningQuery(configuration.Events);
                cmd.CommandType = CommandType.Text;
                cmd.Parameters.Add(new SqlParameter("@sessionName", SqlDbType.NVarChar)
                {
                    Value = configuration.SessionName
                });

                await cmd.ExecuteNonQueryAsync(token);
            },
            _configuration,
            cancellationToken);
    }

    public async Task DropSession(CancellationToken cancellationToken)
    {
        await _resiliencePipeline.ExecuteAsync(static async (configuration, token) =>
            {
                await using var connection = new SqlConnection(configuration.ConnectionString);
                await connection.OpenAsync(token);
                await using var cmd = connection.CreateCommand();

                cmd.CommandText = XEventSessionQueries.DropQuery();
                cmd.CommandType = CommandType.Text;
                cmd.Parameters.Add(new SqlParameter("@sessionName", SqlDbType.NVarChar)
                {
                    Value = configuration.SessionName
                });

                await cmd.ExecuteNonQueryAsync(token);
            },
            _configuration,
            cancellationToken);
    }
}