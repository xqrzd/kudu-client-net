namespace Knet.Kudu.Client.Util;

public interface ISystemClock
{
    /// <summary>
    /// Retrieve the current milliseconds. This value should only
    /// be used to measure how much time has passed relative to
    /// another call to this property.
    /// </summary>
    long CurrentMilliseconds { get; }
}
