namespace Knet.Kudu.Client.Internal
{
    public interface ISystemClock
    {
        long CurrentMilliseconds { get; }
    }
}
