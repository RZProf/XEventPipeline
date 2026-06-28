using System.Buffers;
using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace XEventPipeline;

public static class ChannelExtensions
{
    public static async IAsyncEnumerable<Batch<T>> IntoBatches<T>(
        this ChannelReader<T> reader,
        int batchSize,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var buffer = ArrayPool<T>.Shared.Rent(batchSize);
        var count = 0;

        while (!cancellationToken.IsCancellationRequested)
        {
            T item;
            try
            {
                item = await reader.ReadAsync(cancellationToken);
            }
            catch (ChannelClosedException)
            {
                break;
            }
            catch (OperationCanceledException)
            {
                break;
            }

            buffer[count++] = item;

            while (count < batchSize && reader.TryRead(out var extraItem)) buffer[count++] = extraItem;

            if (count == batchSize)
            {
                yield return new Batch<T>(buffer, count);
                buffer = ArrayPool<T>.Shared.Rent(batchSize);
                count = 0;
            }
        }

        if (count > 0)
            yield return new Batch<T>(buffer, count);
        else
            ArrayPool<T>.Shared.Return(buffer);
    }

    public struct Batch<T> : IDisposable
    {
        private T[]? _array;

        public int Count { get; }

        public T? this[int index] => _array is null ? default : _array[index];

        public Batch(T[]? array, int count)
        {
            _array = array;
            Count = count;
        }

        public void Dispose()
        {
            var arr = _array;
            if (arr == null)
                return;

            _array = null;
            ArrayPool<T>.Shared.Return(arr);
        }
    }
}