using System.Buffers;
using System.Buffers.Binary;
using System.IO.Pipelines;
using System.Text;
using Microsoft.SqlServer.XEvent.XELite;
using SpanJson;

namespace XEventPipeline.XEventSinks.Postgres;

public static class PostgresXEventEncoder
{
    private static readonly DateTimeOffset PgEpoch = new(2000, 1, 1, 0, 0, 0, TimeSpan.Zero);

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
            var totalRowSize = 2 + 20 + 4 + nameByteCount + 12 + 36 + 4 + actions.Count + 1 + 4 + fields.Count + 1;

            var memory = writer.GetMemory(totalRowSize);
            var span = memory.Span;
            var bytesWritten = 0;

            BinaryPrimitives.WriteInt16BigEndian(span[bytesWritten..], 8);
            bytesWritten += 2;

            BinaryPrimitives.WriteInt32BigEndian(span[bytesWritten..], 16);
            bytesWritten += 4;

            xEvent.UUID.TryWriteBytes(span[bytesWritten..], true, out var uuidBytes);
            bytesWritten += uuidBytes;

            BinaryPrimitives.WriteInt32BigEndian(span[bytesWritten..], nameByteCount);
            bytesWritten += 4;
            Encoding.UTF8.GetBytes(xEvent.Name, span[bytesWritten..]);
            bytesWritten += nameByteCount;

            BinaryPrimitives.WriteInt32BigEndian(span[bytesWritten..], 8);
            bytesWritten += 4;
            var microseconds = (xEvent.Timestamp.ToUniversalTime() - PgEpoch).Ticks / 10;
            BinaryPrimitives.WriteInt64BigEndian(span[bytesWritten..], microseconds);
            bytesWritten += 8;

            BinaryPrimitives.WriteInt32BigEndian(span[bytesWritten..], 8);
            BinaryPrimitives.WriteInt64BigEndian(span[(bytesWritten + 4)..], xEvent.XEventStartOffsetInBytes);
            bytesWritten += 12;

            BinaryPrimitives.WriteInt32BigEndian(span[bytesWritten..], 8);
            BinaryPrimitives.WriteInt64BigEndian(span[(bytesWritten + 4)..], xEvent.XEventEndOffsetInBytes);
            bytesWritten += 12;

            BinaryPrimitives.WriteInt32BigEndian(span[bytesWritten..], 8);
            BinaryPrimitives.WriteInt64BigEndian(span[(bytesWritten + 4)..], xEvent.XEventSizeInBytes);
            bytesWritten += 12;

            BinaryPrimitives.WriteInt32BigEndian(span[bytesWritten..], actions.Count + 1);
            bytesWritten += 4;
            span[bytesWritten] = 0x01;
            bytesWritten += 1;
            actions.Array.AsSpan(0, actions.Count).CopyTo(span[bytesWritten..]);
            bytesWritten += actions.Count;

            BinaryPrimitives.WriteInt32BigEndian(span[bytesWritten..], fields.Count + 1);
            bytesWritten += 4;
            span[bytesWritten] = 0x01;
            bytesWritten += 1;
            fields.Array.AsSpan(0, fields.Count).CopyTo(span[bytesWritten..]);
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
}