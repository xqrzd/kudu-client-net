using System;
using System.Buffers;
using System.Runtime.CompilerServices;

#nullable enable

// https://github.com/CommunityToolkit/WindowsCommunityToolkit/blob/main/Microsoft.Toolkit.HighPerformance/Buffers/ArrayPoolBufferWriter%7BT%7D.cs

namespace Knet.Kudu.Client.Internal;

internal sealed class ArrayPoolBufferWriter<T> : IBufferWriter<T>, IDisposable
{
    /// <summary>
    /// The default buffer size to use to expand empty arrays.
    /// </summary>
    private const int DefaultInitialBufferSize = 256;

    /// <summary>
    /// The underlying <typeparamref name="T"/> array.
    /// </summary>
    private T[]? _array;

    /// <summary>
    /// The starting offset within <see cref="_array"/>.
    /// </summary>
    private int _index;

    /// <summary>
    /// Initializes a new instance of the <see cref="ArrayPoolBufferWriter{T}"/> class.
    /// </summary>
    public ArrayPoolBufferWriter() : this(DefaultInitialBufferSize)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="ArrayPoolBufferWriter{T}"/> class.
    /// </summary>
    /// <param name="initialCapacity">
    /// The minimum capacity with which to initialize the underlying buffer.
    /// </param>
    public ArrayPoolBufferWriter(int initialCapacity)
    {
        _array = ArrayPool<T>.Shared.Rent(initialCapacity);
    }

    public Memory<T> WrittenMemory
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get
        {
            var array = _array;

            if (array is null)
            {
                ThrowObjectDisposedException();
            }

            return array.AsMemory(0, _index);
        }
    }

    public Span<T> WrittenSpan
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get
        {
            var array = _array;

            if (array is null)
            {
                ThrowObjectDisposedException();
            }

            return array.AsSpan(0, _index);
        }
    }

    public T[] WrittenArray
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get
        {
            var array = _array;

            if (array is null)
            {
                ThrowObjectDisposedException();
            }

            return array!;
        }
    }

    public int WrittenCount => _index;

    public void Dispose()
    {
        var array = _array;

        if (array is null)
        {
            return;
        }

        _array = null;
        ArrayPool<T>.Shared.Return(array);
    }

    public void Advance(int count)
    {
        var array = _array;

        if (array is null)
        {
            ThrowObjectDisposedException();
        }

        if (count < 0)
        {
            ThrowArgumentOutOfRangeExceptionForNegativeCount();
        }

        if (_index > array!.Length - count)
        {
            ThrowArgumentExceptionForAdvancedTooFar();
        }

        _index += count;
    }

    public Memory<T> GetMemory(int sizeHint = 0)
    {
        CheckBufferAndEnsureCapacity(sizeHint);

        return _array.AsMemory(_index);
    }

    public Span<T> GetSpan(int sizeHint = 0)
    {
        CheckBufferAndEnsureCapacity(sizeHint);

        return _array.AsSpan(_index);
    }

    /// <summary>
    /// Ensures that <see cref="_array"/> has enough free space to contain a given
    /// number of new items.
    /// </summary>
    /// <param name="sizeHint">
    /// The minimum number of items to ensure space for in <see cref="_array"/>.
    /// </param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void CheckBufferAndEnsureCapacity(int sizeHint)
    {
        var array = _array;

        if (array is null)
        {
            ThrowObjectDisposedException();
        }

        if (sizeHint < 0)
        {
            ThrowArgumentOutOfRangeExceptionForNegativeSizeHint();
        }

        if (sizeHint == 0)
        {
            sizeHint = 1;
        }

        if (sizeHint > array!.Length - _index)
        {
            ResizeBuffer(sizeHint);
        }
    }

    /// <summary>
    /// Resizes <see cref="_array"/> to ensure it can fit the specified number of new items.
    /// </summary>
    /// <param name="sizeHint">
    /// The minimum number of items to ensure space for in <see cref="_array"/>.
    /// </param>
    [MethodImpl(MethodImplOptions.NoInlining)]
    private void ResizeBuffer(int sizeHint)
    {
        int currentSize = _index;
        int newSize = currentSize + sizeHint;

        var newArray = ArrayPool<T>.Shared.Rent(newSize);
        Array.Copy(_array!, 0, newArray, 0, currentSize);
        ArrayPool<T>.Shared.Return(_array!);

        _array = newArray;
    }

    private static void ThrowArgumentOutOfRangeExceptionForNegativeCount()
    {
        throw new ArgumentOutOfRangeException("count", "The count can't be a negative value");
    }

    private static void ThrowArgumentOutOfRangeExceptionForNegativeSizeHint()
    {
        throw new ArgumentOutOfRangeException("sizeHint", "The size hint can't be a negative value");
    }

    private static void ThrowArgumentExceptionForAdvancedTooFar()
    {
        throw new ArgumentException("The buffer writer has advanced too far");
    }

    private static void ThrowObjectDisposedException()
    {
        throw new ObjectDisposedException("The current buffer has already been disposed");
    }
}
