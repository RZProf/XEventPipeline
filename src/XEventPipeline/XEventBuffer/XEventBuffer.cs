using System.Threading.Channels;
using Microsoft.SqlServer.XEvent.XELite;

namespace XEventPipeline.XEventBuffer;

public class XEventBuffer
{
    public XEventBuffer(int capacity)
    {
        var channel = Channel.CreateBounded<IXEvent>(new BoundedChannelOptions(capacity)
        {
            FullMode = BoundedChannelFullMode.DropOldest
        });

        Reader = new XEventBufferReader(channel.Reader);
        Writer = new XEventBufferWriter(channel.Writer);
    }

    public XEventBufferWriter Writer { get; }

    public XEventBufferReader Reader { get; }
}
