using Microsoft.Data.SqlClient;

namespace XEventPipeline.Setup;

public sealed class XEventCatalogService
{
    public async Task<IReadOnlyList<XEventDefinition>> LoadAsync(string connectionString, CancellationToken cancellationToken)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        var events = new Dictionary<(Guid PackageId, string Name), XEventDefinition>();
        await using (var command = connection.CreateCommand())
        {
            command.CommandText = """
                SELECT p.guid, p.name, o.name, COALESCE(o.description, '')
                FROM sys.dm_xe_objects o
                INNER JOIN sys.dm_xe_packages p ON p.guid = o.package_guid
                WHERE o.object_type = 'event'
                ORDER BY p.name, o.name;
                """;
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                var item = new XEventDefinition(reader.GetGuid(0), reader.GetString(1), reader.GetString(2), reader.GetString(3));
                events[(item.PackageId, item.Name)] = item;
            }
        }

        await using (var command = connection.CreateCommand())
        {
            command.CommandText = """
                SELECT object_package_guid, object_name, name, column_type, type_name, COALESCE(description, ''),
                       COALESCE(CONVERT(nvarchar(4000), column_value), '')
                FROM sys.dm_xe_object_columns
                WHERE column_type IN ('data', 'customizable')
                ORDER BY object_package_guid, object_name, column_type, name;
                """;
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                var packageId = reader.GetGuid(0);
                var eventName = reader.GetString(1);
                var column = new XEventColumn(reader.GetString(2), reader.GetString(3), reader.GetString(4), reader.GetString(5), reader.GetString(6));
                if (!events.TryGetValue((packageId, eventName), out var item)) continue;
                if (column.Type == "data") item.Fields.Add(column);
                else item.CustomizableAttributes.Add(column);
            }
        }

        var actions = new List<XEventAction>();
        await using (var command = connection.CreateCommand())
        {
            command.CommandText = """
                SELECT p.name, o.name, COALESCE(o.description, '')
                FROM sys.dm_xe_objects o
                INNER JOIN sys.dm_xe_packages p ON p.guid = o.package_guid
                WHERE o.object_type = 'action'
                ORDER BY p.name, o.name;
                """;
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
                actions.Add(new XEventAction(reader.GetString(0), reader.GetString(1), reader.GetString(2)));
        }

        foreach (var item in events.Values) item.Actions = actions;
        return events.Values.OrderBy(e => e.Package).ThenBy(e => e.Name).ToArray();
    }
}

public sealed class XEventDefinition(Guid packageId, string package, string name, string description)
{
    public string Package { get; } = package;
    public string Name { get; } = name;
    public string Description { get; } = description;
    public string FullName => $"{Package}.{Name}";
    public Guid PackageId { get; } = packageId;
    public List<XEventColumn> Fields { get; } = [];
    public List<XEventColumn> CustomizableAttributes { get; } = [];
    public IReadOnlyList<XEventAction> Actions { get; set; } = [];
}

public sealed record XEventColumn(string Name, string Type, string ValueType, string Description, string DefaultValue)
{
    public bool IsBoolean => ValueType.Equals("boolean", StringComparison.OrdinalIgnoreCase);
}
public sealed record XEventAction(string Package, string Name, string Description)
{
    public string FullName => $"{Package}.{Name}";
}
