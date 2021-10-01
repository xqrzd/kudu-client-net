using System;
using System.Buffers;
using Google.Protobuf.Collections;
using Knet.Kudu.Client.Connection;
using Knet.Kudu.Client.Protocol;

namespace Knet.Kudu.Client.Requests;

// TODO: These types need a lot of refactoring.
public abstract class KuduRpc
{
    // Service names.
    protected const string MasterServiceName = "kudu.master.MasterService";
    protected const string TabletServerServiceName = "kudu.tserver.TabletServerService";
    protected const string TxnManagerServiceName = "kudu.transactions.TxnManagerService";

    public string ServiceName { get; init; } = null!;

    public string MethodName { get; init; } = null!;

    /// <summary>
    /// Returns the set of application-specific feature flags required to service the RPC.
    /// </summary>
    public RepeatedField<uint>? RequiredFeatures { get; init; }

    /// <summary>
    /// The external consistency mode for this RPC.
    /// </summary>
    public ExternalConsistencyMode ExternalConsistencyMode { get; init; } =
        ExternalConsistencyMode.ClientPropagated;

    /// <summary>
    /// The number of times this RPC has been retried.
    /// </summary>
    internal int Attempt { get; set; }

    /// <summary>
    /// The last exception when handling this RPC.
    /// </summary>
    internal Exception? Exception { get; set; }

    /// <summary>
    /// If this RPC needs to be tracked on the client and server-side.
    /// Some RPCs require exactly-once semantics which is enabled by tracking them.
    /// </summary>
    public bool IsRequestTracked { get; init; }

    internal long SequenceId { get; set; } = RequestTracker.NoSeqNo;

    public abstract int CalculateSize();

    public abstract void WriteTo(IBufferWriter<byte> output);

    public abstract void ParseResponse(KuduMessage message);
}

public abstract class KuduRpc<T> : KuduRpc
{
    public T? Output { get; protected set; }
}
