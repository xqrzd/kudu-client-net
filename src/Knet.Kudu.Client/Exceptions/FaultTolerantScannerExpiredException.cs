namespace Knet.Kudu.Client.Exceptions;

/// <summary>
/// A scanner expired exception only used for fault tolerant scanner.
/// </summary>
public class FaultTolerantScannerExpiredException : NonRecoverableException
{
    public FaultTolerantScannerExpiredException(KuduStatus status)
        : base(status)
    {
    }
}
