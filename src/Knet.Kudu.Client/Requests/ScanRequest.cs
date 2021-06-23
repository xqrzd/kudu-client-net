using System.Buffers;
using Google.Protobuf;
using Google.Protobuf.Collections;
using Knet.Kudu.Client.Connection;
using Knet.Kudu.Client.Exceptions;
using Knet.Kudu.Client.Protobuf;
using Knet.Kudu.Client.Protobuf.Tserver;
using Knet.Kudu.Client.Scanner;
using Knet.Kudu.Client.Tablet;

namespace Knet.Kudu.Client.Requests
{
    public class ScanRequest<T> : KuduTabletRpc<ScanResponse<T>>
    {
        private static readonly RepeatedField<uint> _columnPredicateRequiredFeature = new()
        {
            (uint)TabletServerFeatures.ColumnPredicates
        };

        private readonly ScanRequestState _state;
        private readonly ScanRequestPB _scanRequestPb;
        private readonly KuduSchema _schema;
        private readonly IKuduScanParser<T> _parser;
        private readonly bool _isFaultTolerant;

        private ScanResponsePB _responsePb;
        private KuduSidecars _sidecars;

        public ScanRequest(
            ScanRequestState state,
            ScanRequestPB scanRequestPb,
            KuduSchema schema,
            IKuduScanParser<T> parser,
            ReplicaSelection replicaSelection,
            string tableId,
            RemoteTablet tablet,
            byte[] partitionKey,
            bool isFaultTolerant)
        {
            _state = state;
            _scanRequestPb = scanRequestPb;
            _schema = schema;
            _parser = parser;
            _isFaultTolerant = isFaultTolerant;

            ReplicaSelection = replicaSelection;
            TableId = tableId;
            Tablet = tablet;
            PartitionKey = partitionKey;
            NeedsAuthzToken = true;

            if (scanRequestPb.NewScanRequest != null &&
                scanRequestPb.NewScanRequest.ColumnPredicates.Count > 0)
            {
                RequiredFeatures = _columnPredicateRequiredFeature;
            }
        }

        public override string MethodName => "Scan";

        public override ReplicaSelection ReplicaSelection { get; }

        public override int CalculateSize()
        {
            if (_state == ScanRequestState.Opening)
            {
                var newRequest = _scanRequestPb.NewScanRequest;

                // Set tabletId here, as we don't know what tablet the request is
                // going to until GetTabletAsync() is called and that tablet is
                // set on this RPC.
                newRequest.TabletId = ByteString.CopyFromUtf8(Tablet.TabletId);

                if (AuthzToken != null)
                    newRequest.AuthzToken = AuthzToken;

                // If the last propagated timestamp is set, send it with the scan.
                if (newRequest.ReadMode != ReadModePB.ReadYourWrites &&
                    PropagatedTimestamp != KuduClient.NoTimestamp)
                {
                    newRequest.PropagatedTimestamp = (ulong)PropagatedTimestamp;
                }
            }

            return _scanRequestPb.CalculateSize();
        }

        public override void WriteTo(IBufferWriter<byte> output) => _scanRequestPb.WriteTo(output);

        public override void ParseProtobuf(ReadOnlySequence<byte> buffer)
        {
            var response = ScanResponsePB.Parser.ParseFrom(buffer);
            var error = response.Error;

            Error = error;
            _responsePb = response;

            if (error is null)
            {
                return;
            }

            switch (error.Code)
            {
                case TabletServerErrorPB.Types.Code.TabletNotFound:
                case TabletServerErrorPB.Types.Code.TabletNotRunning:
                    if (_state == ScanRequestState.Opening ||
                        (_state == ScanRequestState.Next && _isFaultTolerant))
                    {
                        // Doing this will trigger finding the new location.
                        return;
                    }
                    else
                    {
                        var statusIncomplete = KuduStatus.Incomplete("Cannot continue scanning, " +
                            "the tablet has moved and this isn't a fault tolerant scan");
                        throw new NonRecoverableException(statusIncomplete);
                    }
                case TabletServerErrorPB.Types.Code.ScannerExpired:
                    if (_isFaultTolerant)
                    {
                        var status = KuduStatus.FromTabletServerErrorPB(error);
                        throw new FaultTolerantScannerExpiredException(status);
                    }

                    break;
            }
        }

        public override ScanResponse<T> Output
        {
            get
            {
                var result = _parser.ParseSidecars(_schema, _responsePb, _sidecars);

                var scannerId = _responsePb.HasScannerId
                    ? _responsePb.ScannerId.ToByteArray() : null;

                var scanTimestamp = _responsePb.HasSnapTimestamp
                    ? (long)_responsePb.SnapTimestamp : KuduClient.NoTimestamp;

                var propagatedTimestamp = _responsePb.HasPropagatedTimestamp
                    ? (long)_responsePb.PropagatedTimestamp : KuduClient.NoTimestamp;

                var lastPrimaryKey = _responsePb.HasLastPrimaryKey
                    ? _responsePb.LastPrimaryKey.ToByteArray() : null;

                return new ScanResponse<T>(
                    scannerId,
                    result.Result,
                    result.NumRows,
                    _responsePb.HasMoreResults,
                    scanTimestamp,
                    propagatedTimestamp,
                    lastPrimaryKey,
                    _responsePb.ResourceMetrics);
            }
        }

        public override void ParseSidecars(KuduSidecars sidecars)
        {
            _sidecars = sidecars;
        }
    }

    public enum ScanRequestState
    {
        Opening,
        Next,
        Closing
    }
}
