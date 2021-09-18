namespace Knet.Kudu.Client;

public record KuduTransactionSerializationOptions
{
    /// <summary>
    /// Whether the <see cref="KuduTransaction"/> created from these
    /// options will send keepalive messages to avoid automatic rollback
    /// of the underlying transaction.
    /// </summary>
    public bool EnableKeepalive { get; init; }
}
