using Knet.Kudu.Client.Protocol.Rpc;

namespace Knet.Kudu.Client.Exceptions
{
    public class RpcRemoteException : NonRecoverableException
    {
        public ErrorStatusPB ErrorPb { get; }

        public RpcRemoteException(KuduStatus status, ErrorStatusPB errorPb)
            : base(status)
        {
            ErrorPb = errorPb;
        }
    }
}
