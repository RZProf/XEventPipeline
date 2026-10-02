using System.Data.Common;
using Microsoft.Data.SqlClient;
using Npgsql;

namespace XEventPipeline.Setup;

public static class ConnectionStringFactory
{
    public static string BuildSqlServer(
        string host,
        int port,
        string database,
        string username,
        string password,
        bool trustServerCertificate)
    {
        RequireHost(host, "SQL Server");
        ValidatePort(port, "SQL Server");

        var builder = new SqlConnectionStringBuilder
        {
            DataSource = $"{host.Trim()},{port}",
            InitialCatalog = string.IsNullOrWhiteSpace(database) ? "master" : database.Trim(),
            UserID = username,
            Password = password,
            Encrypt = true,
            TrustServerCertificate = trustServerCertificate
        };
        return builder.ConnectionString;
    }

    public static string BuildPostgres(
        string host,
        int port,
        string database,
        string username,
        string password,
        string sslMode)
    {
        RequireHost(host, "PostgreSQL");
        ValidatePort(port, "PostgreSQL");

        var builder = new NpgsqlConnectionStringBuilder
        {
            Host = host.Trim(),
            Port = port,
            Database = database,
            Username = username,
            Password = password,
            SslMode = Enum.Parse<SslMode>(sslMode, ignoreCase: true)
        };
        return builder.ConnectionString;
    }

    public static string BuildClickHouse(
        string host,
        int port,
        string database,
        string username,
        string password,
        string protocol)
    {
        RequireHost(host, "ClickHouse");
        ValidatePort(port, "ClickHouse");

        var builder = new DbConnectionStringBuilder
        {
            ["Host"] = host.Trim(),
            ["Port"] = port,
            ["Database"] = string.IsNullOrWhiteSpace(database) ? "default" : database.Trim(),
            ["Username"] = username,
            ["Password"] = password,
            ["Protocol"] = protocol
        };
        return builder.ConnectionString;
    }

    private static void RequireHost(string host, string service)
    {
        if (string.IsNullOrWhiteSpace(host))
            throw new ArgumentException($"Enter a {service} host name or address.");
    }

    private static void ValidatePort(int port, string service)
    {
        if (port is < 1 or > 65535)
            throw new ArgumentOutOfRangeException(nameof(port), $"Enter a valid {service} port from 1 to 65535.");
    }
}
