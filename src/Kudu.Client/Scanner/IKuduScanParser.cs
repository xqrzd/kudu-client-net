using System;
using System.Buffers;
using Kudu.Client.Connection;
using Kudu.Client.Protocol.Tserver;

namespace Kudu.Client.Scanner
{
    public interface IKuduScanParser<T> : IDisposable
    {
        T Output { get; }

        void BeginProcessingSidecars(
            Schema scanSchema,
            ScanResponsePB scanResponse,
            KuduSidecarOffsets sidecars);

        void ParseSidecarSegment(ReadOnlySequence<byte> buffer);
    }
}
