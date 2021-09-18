using System;
using System.Buffers;

namespace Knet.Kudu.Client.Internal;

internal sealed class ArrayPoolBuffer<T> : IDisposable
{
    public T[] Buffer { get; private set; }

    public ArrayPoolBuffer(int minimumLength)
    {
        Buffer = ArrayPool<T>.Shared.Rent(minimumLength);
    }

    public void Dispose()
    {
        var buffer = Buffer;
        if (buffer is not null)
        {
            Buffer = null;
            ArrayPool<T>.Shared.Return(buffer);
        }
    }
}
