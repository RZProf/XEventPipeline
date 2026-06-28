namespace XEventPipeline.IntegrationTests;

public class PersistedXEvent : IEquatable<PersistedXEvent>
{
    public Guid Uuid { get; init; }

    public string Name
    {
        get => field ?? string.Empty;
        init;
    }

    public DateTime Timestamp { get; init; }
    public long XEventStartOffsetInBytes { get; init; }
    public long XEventEndOffsetInBytes { get; init; }
    public long XEventSizeInBytes { get; init; }

    public IReadOnlyDictionary<string, object> Fields
    {
        get => field ?? new Dictionary<string, object>();
        init;
    }

    public IReadOnlyDictionary<string, object> Actions
    {
        get => field ?? new Dictionary<string, object>();
        init;
    }

    public bool Equals(PersistedXEvent? other)
    {
        if (other is null)
            return false;

        if (ReferenceEquals(this, other))
            return true;

        return Uuid.Equals(other.Uuid) && Name == other.Name && Timestamp.Equals(other.Timestamp) &&
               XEventStartOffsetInBytes == other.XEventStartOffsetInBytes &&
               XEventEndOffsetInBytes == other.XEventEndOffsetInBytes && XEventSizeInBytes == other.XEventSizeInBytes &&
               Fields.Count == other.Fields.Count && Fields.All(f =>
                   other.Fields.TryGetValue(f.Key, out var otherF) && Equals(f.Value, otherF)) &&
               Actions.Count == other.Actions.Count && Actions.All(a =>
                   other.Actions.TryGetValue(a.Key, out var otherA) && Equals(a.Value, otherA));
    }

    public override bool Equals(object? obj)
    {
        if (obj is null)
            return false;

        if (ReferenceEquals(this, obj))
            return true;

        if (obj.GetType() != GetType())
            return false;

        return Equals((PersistedXEvent)obj);
    }

    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(Uuid);
        hash.Add(Name);
        hash.Add(Timestamp);
        hash.Add(XEventStartOffsetInBytes);
        hash.Add(XEventEndOffsetInBytes);
        hash.Add(XEventSizeInBytes);
        
        foreach (var kvp in Fields.OrderBy(k => k.Key))
        {
            hash.Add(kvp.Key);
            hash.Add(kvp.Value);
        }
        foreach (var kvp in Actions.OrderBy(k => k.Key))
        {
            hash.Add(kvp.Key);
            hash.Add(kvp.Value);
        }

        return hash.ToHashCode();
    }
}