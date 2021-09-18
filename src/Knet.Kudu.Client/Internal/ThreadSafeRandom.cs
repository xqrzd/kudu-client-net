using System;
using System.Threading;

namespace Knet.Kudu.Client.Internal;

internal static class ThreadSafeRandom
{
#if NET6_0_OR_GREATER
    public static Random Instance => Random.Shared;
#else
    private static int _seed = Environment.TickCount;

    private static readonly ThreadLocal<Random> _random =
        new(() => new Random(Interlocked.Increment(ref _seed)));

    public static Random Instance => _random.Value;
#endif
}
