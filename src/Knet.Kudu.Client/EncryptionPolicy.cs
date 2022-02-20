namespace Knet.Kudu.Client;

public enum EncryptionPolicy
{
    // Optional, it uses encrypted connection if the server supports it,
    // but it can connect to insecure servers too.
    Optional,
    // Only connects to remote servers that support encryption, fails
    // otherwise. It can connect to insecure servers only locally.
    RequiredRemote,
    // Only connects to any server, including on the loopback interface,
    // that support encryption, fails otherwise.
    Required
}
