using System;
using System.Buffers;
using System.Collections.Generic;
using Knet.Kudu.Client.Connection;
using Knet.Kudu.Client.Exceptions;
using Knet.Kudu.Client.Protocol.Tserver;

namespace Knet.Kudu.Client.Scanner
{
    public class ResultSetScanParser : IKuduScanParser<ResultSet>
    {
        private int length;
        int nextSidecarIndex;
        Memory<byte> currentSidecar;
        long remaining;
        long remainingSidecarLength;
        private List<ArrayMemoryPoolBuffer<byte>> _sidecars;
        private Schema _scanSchema;
        private ScanResponsePB _responsePB;

        public ResultSet Output { get; private set; }

        public void Dispose()
        {
            var sidecars = _sidecars;
            if (sidecars != null)
            {
                _sidecars = null;

                foreach (var sidecar in sidecars)
                    sidecar.Dispose();
            }
        }

        public void BeginProcessingSidecars(
            Schema scanSchema,
            ScanResponsePB scanResponse,
            KuduSidecarOffsets sidecars)
        {
            _scanSchema = scanSchema;
            _responsePB = scanResponse;
            _sidecars = new List<ArrayMemoryPoolBuffer<byte>>(sidecars.SidecarCount);

            int total = 0;
            length = sidecars.TotalSize;
            for (int i = 0; i < sidecars.SidecarCount; i++)
            {
                var offset = sidecars.GetOffset(i);
                int size;
                if (i + 1 >= sidecars.SidecarCount)
                {
                    size = length - total;
                }
                else
                {
                    var next = sidecars.GetOffset(i + 1);
                    size = next - offset;
                }

                var sidecar = new ArrayMemoryPoolBuffer<byte>(size);
                _sidecars.Add(sidecar);

                total += size;
            }

            if (total != length)
            {
                throw new NonRecoverableException(KuduStatus.IllegalState(
                    $"Expected {length} sidecar bytes, computed {total}"));
            }

            nextSidecarIndex = 0;
            currentSidecar = _sidecars[nextSidecarIndex++].Memory;

            remaining = length;
            remainingSidecarLength = currentSidecar.Length;
        }

        public void ParseSidecarSegment(ref SequenceReader<byte> reader)
        {
            var buffer = reader.Sequence.Slice(reader.Position);

            do
            {
                if (currentSidecar.Length == 0)
                {
                    currentSidecar = _sidecars[nextSidecarIndex++].Memory;
                    remainingSidecarLength = currentSidecar.Length;
                }

                // How much data can we copy from the buffer?
                var bytesToRead = Math.Min(buffer.Length, remainingSidecarLength);
                buffer.Slice(0, bytesToRead).CopyTo(currentSidecar.Span);

                remaining -= bytesToRead;
                remainingSidecarLength -= bytesToRead;
                currentSidecar = currentSidecar.Slice((int)bytesToRead);

                buffer = buffer.Slice(bytesToRead);
                reader.Advance(bytesToRead);
            }
            while (remaining > 0 && buffer.Length > 0);

            if (remaining == 0)
            {
                var response = _responsePB;

                var rowData = _sidecars[response.Data.RowsSidecar];
                IMemoryOwner<byte> indirectData = null;

                if (response.Data.ShouldSerializeIndirectDataSidecar())
                {
                    indirectData = _sidecars[response.Data.IndirectDataSidecar];
                }

                var resultSet = new ResultSet(
                    _scanSchema,
                    response.Data.NumRows,
                    rowData,
                    indirectData);

                _sidecars.Clear();

                Output = resultSet;
            }
        }

        private sealed class ArrayMemoryPoolBuffer<T> : IMemoryOwner<T>
        {
            private readonly int _length;
            private T[] _array;

            public ArrayMemoryPoolBuffer(int size)
            {
                _length = size;
                _array = ArrayPool<T>.Shared.Rent(size);
            }

            public Memory<T> Memory
            {
                get
                {
                    T[] array = _array;
                    if (array == null)
                    {
                        throw new ObjectDisposedException(nameof(ArrayMemoryPoolBuffer<T>));
                    }

                    return new Memory<T>(array, 0, _length);
                }
            }

            public void Dispose()
            {
                T[] array = _array;
                if (array != null)
                {
                    _array = null;
                    ArrayPool<T>.Shared.Return(array);
                }
            }
        }
    }
}
