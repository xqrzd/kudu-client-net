using System;
using Kudu.Client.Protocol.Rpc;

namespace Kudu.Client.Exceptions
{
    public class RpcException : Exception
    {
        public ErrorStatusPB Error { get; }

        public RpcException(ErrorStatusPB error)
            : base(error.Message)
        {
            Error = error;
        }
    }
}
