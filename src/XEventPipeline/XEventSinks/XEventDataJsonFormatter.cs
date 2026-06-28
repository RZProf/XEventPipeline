using SpanJson;
using SpanJson.Resolvers;

namespace XEventPipeline.XEventSinks;

public class XEventDataJsonFormatter<T, TSymbol> : IJsonFormatter<T, TSymbol> where TSymbol : struct
{
    public static readonly XEventDataJsonFormatter<T, TSymbol> Default = new();

    public void Serialize(ref JsonWriter<TSymbol> writer, T value)
    {
        if (value is null || value is not IReadOnlyDictionary<string, object> dictionary)
        {
            writer.WriteUtf8Null();
            return;
        }

        writer.WriteUtf8Dictionary(dictionary);
    }

    public T Deserialize(ref JsonReader<TSymbol> reader)
    {
        throw new NotImplementedException();
    }
}

public sealed class XEventDataResolver<TSymbol>() : ResolverBase<TSymbol, XEventDataResolver<TSymbol>>(
    new SpanJsonOptions
    {
        NamingConvention = NamingConventions.OriginalCase,
        NullOption = NullOptions.ExcludeNulls
    })
    where TSymbol : struct
{
    public override IJsonFormatter<T, TSymbol> GetFormatter<T>()
    {
        return typeof(T) == typeof(IReadOnlyDictionary<string, object>)
            ? XEventDataJsonFormatter<T, TSymbol>.Default
            : base.GetFormatter<T>();
    }
}

public static class JsonWriterExtensions
{
    extension<T>(ref JsonWriter<T> writer) where T : struct
    {
        public void WriteUtf8Dictionary(IReadOnlyDictionary<string, object> dictionary)
        {
            writer.WriteUtf8BeginObject();

            var items = 0;

            foreach (var (key, value) in dictionary)
            {
                writer.WriteUtf8Name(key);
                writer.WriteUtf8Object(value);

                if (++items < dictionary.Count)
                    writer.WriteUtf8ValueSeparator();
            }

            writer.WriteUtf8EndObject();
        }

        //All supported types in `Microsoft.SqlServer.XEvent.XELite.Internal.XEEventParser`
        private void WriteUtf8Object(object value)
        {
            switch (value)
            {
                case null:
                    writer.WriteUtf8Null();
                    break;
                case byte b:
                    writer.WriteUtf8Byte(b);
                    break;
                case short s:
                    writer.WriteUtf8Int16(s);
                    break;
                case int i:
                    writer.WriteUtf8Int32(i);
                    break;
                case long l:
                    writer.WriteUtf8Int64(l);
                    break;
                case ushort us:
                    writer.WriteUtf8UInt16(us);
                    break;
                case uint ui:
                    writer.WriteUtf8UInt32(ui);
                    break;
                case ulong ul:
                    writer.WriteUtf8UInt64(ul);
                    break;
                case float f:
                    writer.WriteUtf8Single(f);
                    break;
                case double d:
                    writer.WriteUtf8Double(d);
                    break;
                case string s:
                    writer.WriteUtf8String(s);
                    break;
                case DateTimeOffset offset:
                    writer.WriteUtf8DateTimeOffset(offset);
                    break;
                case Guid g:
                    writer.WriteUtf8Guid(g);
                    break;
                case bool b:
                    writer.WriteUtf8Boolean(b);
                    break;
                case byte[] b:
                    writer.WriteUtf8Base64EncodedArray(b);
                    break;
                default:
                    writer.WriteUtf8String(value.ToString());
                    break;
            }
        }
    }
}