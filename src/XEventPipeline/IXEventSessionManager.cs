namespace XEventPipeline;

public interface IXEventSessionManager
{
    Task InitializeSession(CancellationToken cancellationToken);

    Task MakeSureSessionIsAlive(CancellationToken cancellationToken);

    Task DropSession(CancellationToken cancellationToken);
}