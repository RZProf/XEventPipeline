using System.Buffers;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Microsoft.SqlServer.XEvent.XELite;

namespace XEventPipeline.XEventBuffer;

public class XEventBufferReader
{
    private readonly ChannelReader<IXEvent> _channelReader;
    private readonly Action<int> _onBatchDisposal;

    private long _inFlightCount;

    public XEventBufferReader(ChannelReader<IXEvent> channelReader)
    {
        _channelReader = channelReader;
        _onBatchDisposal = count => Interlocked.Add(ref _inFlightCount, -count);
    }

    public Task Completion => _channelReader.Completion;

    public long Count => (_channelReader.CanCount ? _channelReader.Count : 0) + _inFlightCount;

    public async IAsyncEnumerable<XEventBatch> IntoBatches(
        int batchSize,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var buffer = ArrayPool<IXEvent>.Shared.Rent(batchSize);
        var count = 0;

        while (!cancellationToken.IsCancellationRequested)
        {
            IXEvent xEvent;
            try
            {
                xEvent = await _channelReader.ReadAsync(cancellationToken);
            }
            catch (ChannelClosedException)
            {
                break;
            }
            catch (OperationCanceledException)
            {
                break;
            }

            buffer[count++] = xEvent;
            Interlocked.Increment(ref _inFlightCount);

            while (count < batchSize && _channelReader.TryRead(out var extraItem))
            {
                buffer[count++] = extraItem;
                Interlocked.Increment(ref _inFlightCount);
            }

            if (count == batchSize)
            {
                yield return new XEventBatch(buffer, count, _onBatchDisposal);
                buffer = ArrayPool<IXEvent>.Shared.Rent(batchSize);
                count = 0;
            }
        }

        if (count > 0)
            yield return new XEventBatch(buffer, count, _onBatchDisposal);
        else
            ArrayPool<IXEvent>.Shared.Return(buffer);
    }

    public IAsyncEnumerable<IXEvent> ReadAllAsync(CancellationToken cancellationToken)
    {
        return _channelReader.ReadAllAsync(cancellationToken);
    }
}
