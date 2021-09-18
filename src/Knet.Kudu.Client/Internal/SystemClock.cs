using System;
using System.Diagnostics;
using Knet.Kudu.Client.Util;

namespace Knet.Kudu.Client.Internal;

internal sealed class SystemClock : ISystemClock
{
#if NETCOREAPP3_1_OR_GREATER
    public long CurrentMilliseconds => Environment.TickCount64;
#else
    private readonly Stopwatch _stopwatch = Stopwatch.StartNew();

    public long CurrentMilliseconds => _stopwatch.ElapsedMilliseconds;
#endif
}
