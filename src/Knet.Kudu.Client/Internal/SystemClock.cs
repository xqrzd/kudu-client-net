using System;

namespace Knet.Kudu.Client.Internal
{
    public class SystemClock : ISystemClock
    {
        public long CurrentMilliseconds
        {
            get
            {
#if NETCOREAPP3_0
                return Environment.TickCount64;
#else
                // TODO: Is there something better/faster than this?
                return DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
#endif
            }
        }
    }
}
