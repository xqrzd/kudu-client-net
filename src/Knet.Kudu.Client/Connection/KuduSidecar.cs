using System;

namespace Knet.Kudu.Client.Connection
{
    public readonly struct KuduSidecar
    {
        public ReadOnlyMemory<byte> Memory { get; }

        public int Sidecar { get; }

        public KuduSidecar(ReadOnlyMemory<byte> data, int sidecar)
        {
            Memory = data;
            Sidecar = sidecar;
        }
    }
}
