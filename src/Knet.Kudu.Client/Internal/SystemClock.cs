using System;
using System.Diagnostics;

namespace Knet.Kudu.Client.Internal
{
    public sealed class SystemClock : ISystemClock
    {
#if NETCOREAPP3_0
        /// <inheritdoc/>
        public long CurrentMilliseconds => Environment.TickCount64;
#else
        private readonly Stopwatch _stopwatch = Stopwatch.StartNew();

        /// <inheritdoc/>
        public long CurrentMilliseconds => _stopwatch.ElapsedMilliseconds;
#endif
    }
}
