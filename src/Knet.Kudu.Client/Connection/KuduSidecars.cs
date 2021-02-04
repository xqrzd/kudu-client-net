using System;
using System.Buffers;

namespace Knet.Kudu.Client.Connection
{
    public sealed class KuduSidecars : IDisposable
    {
        private readonly IMemoryOwner<byte> _data;
        private readonly KuduSidecarOffset[] _sidecarOffsets;

        public int TotalSize { get; }

        public KuduSidecars(
            IMemoryOwner<byte> data,
            KuduSidecarOffset[] sidecarOffsets,
            int totalSize)
        {
            _data = data;
            _sidecarOffsets = sidecarOffsets;
            TotalSize = totalSize;
        }

        public int NumSidecars => _sidecarOffsets.Length;

        public ReadOnlySpan<KuduSidecarOffset> Offsets => _sidecarOffsets;

        public ReadOnlyMemory<byte> Memory => _data.Memory.Slice(0, TotalSize);

        public ReadOnlySpan<byte> Span => _data.Memory.Span.Slice(0, TotalSize);

        public int GetOffset(int sidecar) => _sidecarOffsets[sidecar].Start;

        public ReadOnlyMemory<byte> GetSidecarMemory(int sidecar)
        {
            var offset = _sidecarOffsets[sidecar];
            var memory = _data.Memory;

            return memory.Slice(offset.Start, offset.Length);
        }

        public ReadOnlySpan<byte> GetSidecarSpan(int sidecar)
        {
            var offset = _sidecarOffsets[sidecar];
            var span = _data.Memory.Span;

            return span.Slice(offset.Start, offset.Length);
        }

        public void Dispose()
        {
            _data.Dispose();
        }
    }

    public readonly struct KuduSidecarOffset
    {
        public int Start { get; }

        public int Length { get; }

        public KuduSidecarOffset(int start, int length)
        {
            Start = start;
            Length = length;
        }
    }
}
