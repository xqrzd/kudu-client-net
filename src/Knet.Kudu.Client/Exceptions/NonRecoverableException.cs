using System;

namespace Knet.Kudu.Client.Exceptions;

/// <summary>
/// An exception that cannot be retried.
/// </summary>
public class NonRecoverableException : KuduException
{
    public NonRecoverableException(KuduStatus status)
        : base(status)
    {
    }

    public NonRecoverableException(KuduStatus status, Exception innerException)
        : base(status, innerException)
    {
    }
}
