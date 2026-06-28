using System.Buffers;
using System.Buffers.Binary;
using System.IO.Pipelines;
using System.Text;
using Microsoft.SqlServer.XEvent.XELite;
using SpanJson;

namespace XEventPipeline.XEventSinks.ClickHouse;

public static class ClickHouseXEventEncoder
{
    public static async Task<bool> WriteXEventToPipe(
        PipeWriter writer,
        IXEvent xEvent,
        CancellationToken cancellationToken)
    {
        var nameByteCount = Encoding.UTF8.GetByteCount(xEvent.Name);

        var actions =
            JsonSerializer.Generic.Utf8
                .SerializeToArrayPool<IReadOnlyDictionary<string, object>, XEventDataResolver<byte>>(xEvent.Actions);
        var fields =
            JsonSerializer.Generic.Utf8
                .SerializeToArrayPool<IReadOnlyDictionary<string, object>, XEventDataResolver<byte>>(xEvent.Fields);

        try
        {
            var maxRowSize = 16 + 5 + nameByteCount + 4 + 24 + 5 + actions.Count + 5 + fields.Count;

            var memory = writer.GetMemory(maxRowSize);
            var span = memory.Span;
            var bytesWritten = 0;

            bytesWritten += WriteGuid(span, xEvent.UUID);

            bytesWritten += WriteVarInt(span[bytesWritten..], (uint)nameByteCount);
            Encoding.UTF8.GetBytes(xEvent.Name, span[bytesWritten..]);
            bytesWritten += nameByteCount;

            BinaryPrimitives.WriteUInt32LittleEndian(span[bytesWritten..], (uint)xEvent.Timestamp.ToUnixTimeSeconds());
            bytesWritten += 4;

            BinaryPrimitives.WriteInt64LittleEndian(span[bytesWritten..], xEvent.XEventStartOffsetInBytes);
            bytesWritten += 8;

            BinaryPrimitives.WriteInt64LittleEndian(span[bytesWritten..], xEvent.XEventEndOffsetInBytes);
            bytesWritten += 8;

            BinaryPrimitives.WriteInt64LittleEndian(span[bytesWritten..], xEvent.XEventSizeInBytes);
            bytesWritten += 8;

            bytesWritten += WriteVarInt(span[bytesWritten..], (uint)actions.Count);
            actions.Array.CopyTo(span[bytesWritten..]);
            bytesWritten += actions.Count;

            bytesWritten += WriteVarInt(span[bytesWritten..], (uint)fields.Count);
            fields.Array.CopyTo(span[bytesWritten..]);
            bytesWritten += fields.Count;

            writer.Advance(bytesWritten);

            var result = await writer.FlushAsync(cancellationToken);

            return !result.IsCompleted;
        }
        finally
        {
            if (actions.Array is not null) ArrayPool<byte>.Shared.Return(actions.Array);
            if (fields.Array is not null) ArrayPool<byte>.Shared.Return(fields.Array);
        }
    }

    private static int WriteVarInt(Span<byte> span, uint value)
    {
        var offset = 0;
        while (value >= 0x80)
        {
            span[offset++] = (byte)(value | 0x80);
            value >>= 7;
        }

        span[offset++] = (byte)value;
        return offset;
    }

    private static int WriteGuid(Span<byte> span, Guid guid)
    {
        Span<byte> temp = stackalloc byte[16];
        guid.TryWriteBytes(temp);

        temp.Slice(6, 2).CopyTo(span.Slice(0, 2));
        temp.Slice(4, 2).CopyTo(span.Slice(2, 2));
        temp.Slice(0, 4).CopyTo(span.Slice(4, 4));
        temp.Slice(8, 8).CopyTo(span.Slice(8, 8));
        span.Slice(8, 8).Reverse();

        return 16;
    }
}