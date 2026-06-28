using Confluent.Kafka;

namespace XEventPipeline.XEventSinks.Kafka;

public class ArraySegmentSerializer : ISerializer<ArraySegment<byte>>
{
    public byte[] Serialize(ArraySegment<byte> data, SerializationContext context)
    {
        if (data.Count == 0)
            return [];

        var result = GC.AllocateUninitializedArray<byte>(data.Count);

        Buffer.BlockCopy(data.Array!, data.Offset, result, 0, data.Count);

        return result;
    }
}