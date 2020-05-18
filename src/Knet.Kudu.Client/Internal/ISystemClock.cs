namespace Knet.Kudu.Client.Internal
{
    public interface ISystemClock
    {
        // TODO: Rename to CurrentMilliseconds?
        long TickCount { get; }
    }
}
