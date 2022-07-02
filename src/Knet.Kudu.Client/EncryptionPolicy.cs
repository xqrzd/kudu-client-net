namespace Knet.Kudu.Client;

public enum EncryptionPolicy
{
    /// <summary>
    /// Optional, it uses encrypted connection if the server supports it,
    /// but it can connect to insecure servers too.
    /// </summary>
    Optional,
    /// <summary>
    /// Only connects to remote servers that support encryption, fails
    /// otherwise. It can connect to insecure servers only locally.
    /// </summary>
    RequiredRemote,
    /// <summary>
    /// Only connects to any server, including on the loopback interface,
    /// that support encryption, fails otherwise.
    /// </summary>
    Required
}
