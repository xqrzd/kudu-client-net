using System;
using Kudu.Client.Protocol.Master;

namespace Kudu.Client.Exceptions
{
    public class MasterException : Exception
    {
        public MasterErrorPB Error { get; }

        public MasterException(MasterErrorPB error)
            : base(error.Status.Message)
        {
            Error = error;
        }
    }
}
