using System.Threading.Channels;
using Microsoft.SqlServer.XEvent.XELite;

namespace XEventPipeline.XEventBuffer;

public class XEventBufferWriter
{
    private readonly ChannelWriter<IXEvent> _channelWriter;

    public XEventBufferWriter(ChannelWriter<IXEvent> channelWriter)
    {
        _channelWriter = channelWriter;
    }

    public void TryComplete()
    {
        _channelWriter.TryComplete();
    }

    public ValueTask WriteAsync(IXEvent xeEvent, CancellationToken cancellationToken)
    {
        return _channelWriter.WriteAsync(xeEvent, cancellationToken);
    }
}
