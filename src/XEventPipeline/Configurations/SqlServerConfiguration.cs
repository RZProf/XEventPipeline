namespace XEventPipeline.Configurations;

public class SqlServerConfiguration
{
    private const string DefaultSessionName = "xe_pipeline";

    public required string ConnectionString { get; set; }

    public string SessionName
    {
        get => field ?? DefaultSessionName;
        set;
    }

    public XEventConfiguration[] Events { get; set; } = [];
}

public class XEventConfiguration
{
    public required string Name { get; set; }

    public required string[] Actions { get; set; }

    public XEventCustomizableAttributeConfiguration[] CustomizableAttributes { get; set; } = [];

    public string? PredicateExpression { get; set; }

    public override string ToString()
    {
        var customizableAttributes = CustomizableAttributes.Length != 0
            ? $"{Environment.NewLine}SET {string.Join(',', CustomizableAttributes)}"
            : null;

        var predicateExpression = string.IsNullOrWhiteSpace(PredicateExpression)
            ? null
            : $"{Environment.NewLine}WHERE ({PredicateExpression})";
        var actions = Actions.Length == 0
            ? null
            : $"{Environment.NewLine}ACTION({string.Join(',', Actions)})";

        return $"""
                EVENT {Name}({customizableAttributes}{actions}{predicateExpression})
                """;
    }
}

public class XEventCustomizableAttributeConfiguration
{
    public required string Name { get; set; }
    public required string Value { get; set; }

    public override string ToString()
    {
        var sqlValue = Value.Trim().ToLowerInvariant() switch
        {
            "true" => "1",
            "false" => "0",
            _ => Value
        };

        return $"{Name}=({sqlValue})";
    }
}
