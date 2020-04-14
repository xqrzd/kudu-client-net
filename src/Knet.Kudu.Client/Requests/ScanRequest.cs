using System;
using System.Buffers;
using System.IO;
using Knet.Kudu.Client.Connection;
using Knet.Kudu.Client.Exceptions;
using Knet.Kudu.Client.Protocol;
using Knet.Kudu.Client.Protocol.Tserver;
using Knet.Kudu.Client.Scanner;
using Knet.Kudu.Client.Tablet;
using Knet.Kudu.Client.Util;
using ProtoBuf;

namespace Knet.Kudu.Client.Requests
{
    public class ScanRequest<T> : KuduTabletRpc<ScanResponse<T>>, IDisposable
    {
        private static readonly uint[] _columnPredicateRequiredFeature = new uint[]
        {
            (uint)TabletServerFeatures.ColumnPredicates
        };

        private readonly ScanRequestState _state;
        private readonly ScanRequestPB _scanRequestPb;
        private readonly KuduSchema _schema;
        private readonly IKuduScanParser<T> _parser;
        private readonly bool _isFaultTolerant;

        private ScanResponsePB _responsePB;

        public ScanRequest(
            ScanRequestState state,
            ScanRequestPB scanRequestPb,
            KuduSchema schema,
            IKuduScanParser<T> parser,
            ReplicaSelection replicaSelection,
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
            TableId = tablet.TableId;
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
            _parser.Dispose();
        }

        public override void Serialize(Stream stream)
        {
            if (_state == ScanRequestState.Opening)
            {
                var newRequest = _scanRequestPb.NewScanRequest;

                newRequest.TabletId = Tablet.TabletId.ToUtf8ByteArray();

                if (AuthzToken != null)
                    newRequest.AuthzToken = AuthzToken;

                // If the last propagated timestamp is set, send it with the scan.
                if (newRequest.ReadMode != ReadModePB.ReadYourWrites &&
                    PropagatedTimestamp != KuduClient.NoTimestamp)
                {
                    newRequest.PropagatedTimestamp = (ulong)PropagatedTimestamp;
                }
            }

            Serializer.SerializeWithLengthPrefix(stream, _scanRequestPb, PrefixStyle.Base128);
        }

        public override void ParseProtobuf(ReadOnlySequence<byte> buffer)
        {
            var response = Serializer.Deserialize<ScanResponsePB>(buffer);
            var error = response.Error;

            Error = error;
            _responsePB = response;

            if (error == null)
                return;

            switch (error.code)
            {
                case TabletServerErrorPB.Code.TabletNotFound:
                case TabletServerErrorPB.Code.TabletNotRunning:
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
                case TabletServerErrorPB.Code.ScannerExpired:
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
                return new ScanResponse<T>(
                    _responsePB.ScannerId,
                    _parser.Output,
                    _responsePB.Data.NumRows,
                    _responsePB.HasMoreResults,
                    _responsePB.ShouldSerializeSnapTimestamp() ? (long)_responsePB.SnapTimestamp : KuduClient.NoTimestamp,
                    _responsePB.ShouldSerializePropagatedTimestamp() ? (long)_responsePB.PropagatedTimestamp : KuduClient.NoTimestamp,
                    _responsePB.LastPrimaryKey);
            }
        }

        public override void BeginProcessingSidecars(KuduSidecarOffsets sidecars)
        {
            _parser.BeginProcessingSidecars(_schema, _responsePB, sidecars);
        }

        public override void ParseSidecarSegment(ref SequenceReader<byte> reader)
        {
            _parser.ParseSidecarSegment(ref reader);
        }
    }

    public enum ScanRequestState
    {
        Opening,
        Next,
        Closing
    }
}
