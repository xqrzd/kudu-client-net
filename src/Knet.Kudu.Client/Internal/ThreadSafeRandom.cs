using System;
using System.Threading;

namespace Knet.Kudu.Client.Internal
{
    internal static class ThreadSafeRandom
    {
        private static int _seed = Environment.TickCount;

        private static readonly ThreadLocal<Random> _random =
            new(() => new Random(Interlocked.Increment(ref _seed)));

        public static Random Instance => _random.Value;
    }
}
