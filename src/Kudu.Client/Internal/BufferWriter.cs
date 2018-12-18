using System;
using System.Buffers;

namespace Kudu.Client.Internal
{
    public class BufferWriter : IBufferWriter<byte>, IMemoryOwner<byte>
    {
        private byte[] _buffer;
        private int _length;
        private int _offset;

        public BufferWriter(int minimumLength)
        {
            _buffer = ArrayPool<byte>.Shared.Rent(minimumLength);
            _length = 0;
            _offset = 0;
        }

        public Memory<byte> Memory => new Memory<byte>(_buffer, 0, _length);

        public int RemainingSize => _buffer.Length - _offset;

        public void Dispose()
        {
            if (_buffer != null)
            {
                ArrayPool<byte>.Shared.Return(_buffer);
                _buffer = null;
            }
        }

        public Span<byte> GetSpan(int sizeHint = 0)
        {
            int length = EnsureCapacity(sizeHint);
            return new Span<byte>(_buffer, _offset, length);
        }

        public Memory<byte> GetMemory(int sizeHint = 0)
        {
            int length = EnsureCapacity(sizeHint);
            return new Memory<byte>(_buffer, _offset, length);
        }

        public void Advance(int count)
        {
            _offset += count;
            _length += count;
        }

        private int EnsureCapacity(int sizeHint)
        {
            // The caller requested all remaining memory.
            if (sizeHint == 0)
                return RemainingSize;

            // Resize the internal array if we don't have enough.
            if (sizeHint > RemainingSize)
                IncreaseBuffer(sizeHint);

            return sizeHint;
        }

        private void IncreaseBuffer(int minimumIncrease)
        {
            int currentSize = _buffer.Length;
            int newSize = Math.Max(currentSize + minimumIncrease, currentSize * 2);

            byte[] newBuffer = ArrayPool<byte>.Shared.Rent(newSize);
            _buffer.CopyTo(newBuffer.AsSpan());
            ArrayPool<byte>.Shared.Return(_buffer);
            _buffer = newBuffer;
        }
    }
}
