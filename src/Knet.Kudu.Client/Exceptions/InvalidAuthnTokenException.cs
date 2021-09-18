namespace Knet.Kudu.Client.Exceptions;

/// <summary>
/// Receiving this exception means the current authentication token is no
/// longer valid and a new one is needed to establish connections to the
/// Kudu servers for sending RPCs.
/// </summary>
public class InvalidAuthnTokenException : RecoverableException
{
    public InvalidAuthnTokenException(KuduStatus status)
        : base(status)
    {
    }
}
