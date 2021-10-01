using System;

namespace Knet.Kudu.Client.Connection;

public class HostAndPort : IEquatable<HostAndPort>
{
    public string Host { get; }

    public int Port { get; }

    public HostAndPort(string host, int port)
    {
        Host = host;
        Port = port;
    }

    public bool Equals(HostAndPort? other)
    {
        if (other is null)
            return false;

        if (ReferenceEquals(this, other))
            return true;

        return StringComparer.OrdinalIgnoreCase.Equals(Host, other.Host) &&
            Port == other.Port;
    }

    public override bool Equals(object? obj) => Equals(obj as HostAndPort);

    public override int GetHashCode() =>
        HashCode.Combine(StringComparer.OrdinalIgnoreCase.GetHashCode(Host), Port);

    public override string ToString() => $"{Host}:{Port}";
}
