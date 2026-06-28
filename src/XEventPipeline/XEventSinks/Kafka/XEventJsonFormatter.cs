using Microsoft.SqlServer.XEvent.XELite;
using SpanJson;
using SpanJson.Resolvers;

namespace XEventPipeline.XEventSinks.Kafka;

public class XEventJsonFormatter<T, TSymbol> : IJsonFormatter<T, TSymbol> where TSymbol : struct
{
    public static readonly XEventJsonFormatter<T, TSymbol> Default = new();

    public void Serialize(ref JsonWriter<TSymbol> writer, T value)
    {
        if (value is null || value is not IXEvent xEvent)
        {
            writer.WriteUtf8Null();
            return;
        }

        writer.WriteUtf8BeginObject();

        writer.WriteUtf8Name("UUID");
        writer.WriteUtf8Guid(xEvent.UUID);
        writer.WriteUtf8ValueSeparator();
        writer.WriteUtf8Name("Name");
        writer.WriteUtf8String(xEvent.Name);
        writer.WriteUtf8ValueSeparator();
        writer.WriteUtf8Name("Timestamp");
        writer.WriteUtf8DateTimeOffset(xEvent.Timestamp);
        writer.WriteUtf8ValueSeparator();
        writer.WriteUtf8Name("XEventStartOffsetInBytes");
        writer.WriteUtf8Int64(xEvent.XEventStartOffsetInBytes);
        writer.WriteUtf8ValueSeparator();
        writer.WriteUtf8Name("XEventEndOffsetInBytes");
        writer.WriteUtf8Int64(xEvent.XEventEndOffsetInBytes);
        writer.WriteUtf8ValueSeparator();
        writer.WriteUtf8Name("XEventSizeInBytes");
        writer.WriteUtf8Int64(xEvent.XEventSizeInBytes);
        writer.WriteUtf8ValueSeparator();
        writer.WriteUtf8Name("Actions");
        writer.WriteUtf8Dictionary(xEvent.Actions);
        writer.WriteUtf8ValueSeparator();
        writer.WriteUtf8Name("Fields");
        writer.WriteUtf8Dictionary(xEvent.Fields);

        writer.WriteUtf8EndObject();
    }

    public T Deserialize(ref JsonReader<TSymbol> reader)
    {
        throw new NotImplementedException();
    }
}

public sealed class XEventResolver<TSymbol>() : ResolverBase<TSymbol, XEventResolver<TSymbol>>(
    new SpanJsonOptions
    {
        NamingConvention = NamingConventions.OriginalCase,
        NullOption = NullOptions.ExcludeNulls
    })
    where TSymbol : struct
{
    public override IJsonFormatter<T, TSymbol> GetFormatter<T>()
    {
        return typeof(T) == typeof(IXEvent)
            ? XEventJsonFormatter<T, TSymbol>.Default
            : base.GetFormatter<T>();
    }
}