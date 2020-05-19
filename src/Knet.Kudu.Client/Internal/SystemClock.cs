using System;
using System.Diagnostics;

namespace Knet.Kudu.Client.Internal
{
#if NETCOREAPP3_0
    public class SystemClock : ISystemClock
    {
        public long CurrentMilliseconds => Environment.TickCount64;
    }
#else
    public class SystemClock : ISystemClock
    {
        private readonly Stopwatch _stopwatch = Stopwatch.StartNew();

        public long CurrentMilliseconds => _stopwatch.ElapsedMilliseconds;
    }
#endif
}
