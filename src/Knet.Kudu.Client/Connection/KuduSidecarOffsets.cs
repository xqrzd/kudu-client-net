namespace Knet.Kudu.Client.Connection
{
    public readonly struct KuduSidecarOffsets
    {
        private readonly uint[] _sidecarOffsets;

        public int TotalSize { get; }

        public KuduSidecarOffsets(uint[] sidecarOffsets, int totalSize)
        {
            _sidecarOffsets = sidecarOffsets;
            TotalSize = totalSize;
        }

        public int SidecarCount => _sidecarOffsets.Length;

        public int GetOffset(int sidecar) =>
            (int)_sidecarOffsets[sidecar];
    }
}
