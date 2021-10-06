using System;
using System.Diagnostics;

namespace Knet.Kudu.Client.Util;

public sealed class SystemClock : ISystemClock
{
#if NETCOREAPP3_1_OR_GREATER
    public long CurrentMilliseconds => Environment.TickCount64;
#else
    private readonly Stopwatch _stopwatch = Stopwatch.StartNew();

    public long CurrentMilliseconds => _stopwatch.ElapsedMilliseconds;
#endif
}
