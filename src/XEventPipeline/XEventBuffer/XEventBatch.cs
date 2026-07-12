using System.Buffers;
using Microsoft.SqlServer.XEvent.XELite;

namespace XEventPipeline.XEventBuffer;

public struct XEventBatch : IDisposable
{
    private IXEvent[]? _array;
    private readonly Action<int> _onDispose;

    public int Count { get; }

    public IXEvent? this[int index] => _array?[index];

    public XEventBatch(IXEvent[]? array, int count, Action<int> onDispose)
    {
        _array = array;
        _onDispose = onDispose;
        Count = count;
    }

    public void Dispose()
    {
        var arr = _array;
        if (arr == null)
            return;

        _array = null;
        ArrayPool<IXEvent>.Shared.Return(arr);

        _onDispose?.Invoke(Count);
    }
}
