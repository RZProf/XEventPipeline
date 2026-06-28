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

    public required string Package { get; set; }

    public required string[] Actions { get; set; }

    public XEventCustomizableAttributeConfiguration[] CustomizableAttributes { get; set; } = [];

    public string? PredicateExpression { get; set; }

    public override string ToString()
    {
        var actions = string.Join(',', Actions.Select(action => $"{Package}.{action}"));
        var customizableAttributes = CustomizableAttributes.Length != 0
            ? $"{Environment.NewLine}SET {string.Join(',', CustomizableAttributes)}"
            : null;

        var predicateExpression = string.IsNullOrWhiteSpace(PredicateExpression)
            ? null
            : $"{Environment.NewLine}WHERE ({PredicateExpression})";

        return $"""
                EVENT {Package}.{Name}({customizableAttributes}
                ACTION({actions}){predicateExpression})
                """;
    }
}

public class XEventCustomizableAttributeConfiguration
{
    public required string Name { get; set; }
    public required string Value { get; set; }

    public override string ToString()
    {
        return $"{Name}=({Value})";
    }
}