using System;
using System.Buffers;
using System.IO;

namespace Kudu.Client.Internal
{
    public sealed class RecyclableMemoryStream : Stream, IMemoryOwner<byte>
    {
        private byte[] _buffer;
        private int _length;
        private int _position;

        public RecyclableMemoryStream() : this(4096)
        {
        }

        public RecyclableMemoryStream(int minimumLength)
        {
            _buffer = ArrayPool<byte>.Shared.Rent(minimumLength);
            _length = 0;
            _position = 0;
        }

        public Memory<byte> Memory => ToMemory();

        public override bool CanRead => true;

        public override bool CanSeek => true;

        public override bool CanWrite => true;

        public int Capacity => _buffer.Length;

        public override long Length => _length;

        public override long Position
        {
            get => _position;
            set => _position = (int)value;
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            switch (origin)
            {
                case SeekOrigin.Begin:
                    {
                        int tempPosition = unchecked((int)offset);
                        _position = tempPosition;
                        break;
                    }
                case SeekOrigin.Current:
                    {
                        int tempPosition = unchecked(_position + (int)offset);
                        _position = tempPosition;
                        break;
                    }
                case SeekOrigin.End:
                    {
                        int tempPosition = unchecked(_length + (int)offset);
                        _position = tempPosition;
                        break;
                    }
                default:
                    throw new ArgumentException("Argument_InvalidSeekOrigin");
            }

            return _position;
        }

        public override void SetLength(long value)
        {
            if (value < 0 || value > int.MaxValue)
            {
                throw new ArgumentOutOfRangeException("value", "ArgumentOutOfRange_StreamLength");
            }

            int newLength = (int)value;
            EnsureCapacity(newLength);
            _length = newLength;
            if (_position > newLength) _position = newLength;
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            var slice = buffer.AsSpan(offset, count);
            return Read(slice);
        }

        public override int Read(Span<byte> buffer)
        {
            var desiredLength = buffer.Length;
            var maxCanRead = _length - _position;
            var readAmount = Math.Min(desiredLength, maxCanRead);

            var slice = _buffer.AsSpan(_position, readAmount);
            slice.CopyTo(buffer);
            _position += readAmount;
            return readAmount;
        }

        public override int ReadByte()
        {
            if (_position >= _length)
                return -1;

            return _buffer[_position++];
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            var slice = buffer.AsSpan(offset, count);
            Write(slice);
        }

        public override void Write(ReadOnlySpan<byte> buffer)
        {
            // Make sure there's space for the buffer.
            EnsureCapacity(buffer.Length);

            var slice = _buffer.AsSpan(_position);
            buffer.CopyTo(slice);

            int i = _position + buffer.Length;
            if (i > _length)
            {
                _length = i;
            }

            _position += buffer.Length;
        }

        public override void WriteByte(byte value)
        {
            if (_position >= _length)
            {
                // Make sure we have space to write 1 byte.
                EnsureCapacity(1);
                _length = _position + 1;
            }

            _buffer[_position++] = value;
        }

        public byte[] GetBuffer() => _buffer;

        public byte[] ToArray() => ToSpan().ToArray();

        public Span<byte> ToSpan() => _buffer.AsSpan(0, _length);

        public Span<byte> AsSpan(int size)
        {
            EnsureCapacity(size);
            var slice = _buffer.AsSpan(_position, size);
            _position += size;
            _length += size;
            return slice;
        }

        public Memory<byte> ToMemory() => _buffer.AsMemory(0, _length);

        public Memory<byte> AsMemory(int size)
        {
            EnsureCapacity(size);
            var slice = _buffer.AsMemory(_position, size);
            _position += size;
            _length += size;
            return slice;
        }

        public override void Flush() { }

        protected override void Dispose(bool disposing)
        {
            try
            {
                if (disposing)
                {
                    if (_buffer != null)
                    {
                        ArrayPool<byte>.Shared.Return(_buffer);
                        _buffer = null;
                    }
                }
            }
            finally
            {
                // Call base.Close() to cleanup async IO resources
                base.Dispose(disposing);
            }
        }

        private void EnsureCapacity(int minimumSize)
        {
            var remainingCapacity = Capacity - _position;
            if (minimumSize > remainingCapacity)
            {
                IncreaseBuffer(minimumSize);
            }
        }

        private void IncreaseBuffer(int minimumIncrease)
        {
            var newSize = Math.Max(minimumIncrease + _buffer.Length, _buffer.Length * 2);
            var newBuffer = ArrayPool<byte>.Shared.Rent(newSize);
            _buffer.CopyTo(newBuffer.AsSpan());
            ArrayPool<byte>.Shared.Return(_buffer);
            _buffer = newBuffer;
        }
    }
}
