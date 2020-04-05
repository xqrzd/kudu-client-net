using System;
using System.Buffers;
using Knet.Kudu.Client.Connection;
using Knet.Kudu.Client.Protocol.Tserver;

namespace Knet.Kudu.Client.Scanner
{
    public interface IKuduScanParser<T> : IDisposable
    {
        T Output { get; }

        void BeginProcessingSidecars(
            KuduSchema scanSchema,
            ScanResponsePB scanResponse,
            KuduSidecarOffsets sidecars);

        void ParseSidecarSegment(ref SequenceReader<byte> reader);
    }
}
