using System;
using System.Diagnostics;

namespace Knet.Kudu.Client.Internal
{
    public class SystemClock : ISystemClock
    {
#if NETCOREAPP3_0
        public long CurrentMilliseconds => Environment.TickCount64;
#else
        private readonly Stopwatch _stopwatch = Stopwatch.StartNew();

        public long CurrentMilliseconds => _stopwatch.ElapsedMilliseconds;
#endif
    }
}
