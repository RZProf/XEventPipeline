using System.Diagnostics.Metrics;
using XEventPipeline.XEventBuffer;

namespace XEventPipeline;

public class XEventPipelineDiagnostics
{
    public const string Name = "XEventPipeline";

    private readonly Meter _meter = new(Name);

    public XEventPipelineDiagnostics(XEventBufferReader bufferReader)
    {
        _meter.CreateObservableGauge(
            "xeventpipeline.queue.size",
            () => bufferReader.Count,
            "{number}",
            "Current number of extended events residing in the internal memory buffer awaiting processing.");
    }
}
