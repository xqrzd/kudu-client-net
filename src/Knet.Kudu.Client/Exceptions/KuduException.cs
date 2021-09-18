using System;

namespace Knet.Kudu.Client.Exceptions;

public abstract class KuduException : Exception
{
    public KuduStatus Status { get; }

    public KuduException(KuduStatus status)
        : base(status.Message)
    {
        Status = status;
    }

    public KuduException(KuduStatus status, Exception innerException)
        : base(status.Message, innerException)
    {
        Status = status;
    }
}
