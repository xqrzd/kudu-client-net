using System;
using System.Buffers;
using Google.Protobuf;
using Google.Protobuf.Collections;
using Knet.Kudu.Client.Exceptions;
using Knet.Kudu.Client.Protobuf;
using Knet.Kudu.Client.Protobuf.Tserver;
using Knet.Kudu.Client.Protocol;
using Knet.Kudu.Client.Scanner;
using Knet.Kudu.Client.Tablet;

namespace Knet.Kudu.Client.Requests
{
    internal class ScanRequest : KuduTabletRpc<ScanResponse>, IDisposable
    {
        private static readonly RepeatedField<uint> _columnPredicateRequiredFeature = new()
        {
            (uint)TabletServerFeatures.ColumnPredicates
        };

        private readonly ScanRequestState _state;
        private readonly ScanRequestPB _scanRequestPb;
        private readonly KuduSchema _schema;
        private readonly bool _isFaultTolerant;

        public ScanRequest(
            ScanRequestState state,
            ScanRequestPB scanRequestPb,
            KuduSchema schema,
            ReplicaSelection replicaSelection,
            string tableId,
            RemoteTablet tablet,
            byte[] partitionKey,
            bool isFaultTolerant)
        {
            _state = state;
            _scanRequestPb = scanRequestPb;
            _schema = schema;
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

        public void Dispose()
        {
            ClearOutput();
        }

        public void TakeOutput()
        {
            Output = null;
        }

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

        public override void ParseResponse(KuduMessage message)
        {
            ClearOutput();

            var response = ScanResponsePB.Parser.ParseFrom(message.MessageProtobuf);
            var error = response.Error;

            Error = error;

            if (error is null)
            {
                Output = GetOutput(response, message);
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

        private ScanResponse GetOutput(ScanResponsePB responsePb, KuduMessage message)
        {
            var scannerId = responsePb.HasScannerId
                ? responsePb.ScannerId.ToByteArray()
                : null;

            var scanTimestamp = responsePb.HasSnapTimestamp
                ? (long)responsePb.SnapTimestamp
                : KuduClient.NoTimestamp;

            var propagatedTimestamp = responsePb.HasPropagatedTimestamp
                ? (long)responsePb.PropagatedTimestamp
                : KuduClient.NoTimestamp;

            var lastPrimaryKey = responsePb.HasLastPrimaryKey
                ? responsePb.LastPrimaryKey.ToByteArray()
                : null;

            var resultSet = ResultSetFactory.Create(_schema, responsePb, message);

            return new ScanResponse(
                scannerId,
                resultSet,
                responsePb.HasMoreResults,
                scanTimestamp,
                propagatedTimestamp,
                lastPrimaryKey,
                responsePb.ResourceMetrics);
        }

        private void ClearOutput()
        {
            var output = Output;
            if (output is not null)
            {
                output = null;
                output.ResultSet.Dispose();
            }
        }
    }

    public enum ScanRequestState
    {
        Opening,
        Next,
        Closing
    }
}
