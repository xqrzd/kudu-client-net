using Knet.Kudu.Client.Protobuf.Transactions;

namespace Knet.Kudu.Client.Requests;

internal abstract class KuduTxnRpc<T> : KuduRpc<T>
{
    public TxnManagerErrorPB? Error { get; protected set; }

    public KuduTxnRpc()
    {
        ServiceName = TxnManagerServiceName;
    }
}
