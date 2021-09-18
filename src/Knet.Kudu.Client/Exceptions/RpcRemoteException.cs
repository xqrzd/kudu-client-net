using Knet.Kudu.Client.Protobuf.Rpc;

namespace Knet.Kudu.Client.Exceptions;

public class RpcRemoteException : NonRecoverableException
{
    public ErrorStatusPB ErrorPb { get; }

    public RpcRemoteException(KuduStatus status, ErrorStatusPB errorPb)
        : base(status)
    {
        ErrorPb = errorPb;
    }
}
