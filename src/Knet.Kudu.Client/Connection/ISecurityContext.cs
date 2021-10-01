using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Security;
using Knet.Kudu.Client.Protobuf.Security;

namespace Knet.Kudu.Client.Connection;

/// <summary>
/// Stores security-related infrastructure, credentials, and trusted certificates.
/// Implementations of this should be thread safe.
/// </summary>
public interface ISecurityContext
{
    /// <summary>
    /// True if the user imported an authentication token to use.
    /// </summary>
    public bool IsAuthenticationTokenImported { get; }

    /// <summary>
    /// Set the token received from connecting to the leader master.
    /// </summary>
    /// <param name="token">The token to set.</param>
    public void SetAuthenticationToken(SignedTokenPB token);

    /// <summary>
    /// Get the current authentication token, or null if we have no valid token.
    /// </summary>
    public SignedTokenPB? GetAuthenticationToken();

    /// <summary>
    /// Export serialized authentication data that may be passed to a different
    /// client instance and imported to provide that client the ability to connect
    /// to the cluster.
    /// </summary>
    public ReadOnlyMemory<byte> ExportAuthenticationCredentials();

    /// <summary>
    /// Import data allowing this client to authenticate to the cluster.
    /// </summary>
    /// <param name="token">The authentication token.</param>
    public void ImportAuthenticationCredentials(ReadOnlyMemory<byte> token);

    /// <summary>
    /// Mark the given CA cert (provided in DER form) as the trusted CA cert for the
    /// client. Replaces any previously trusted cert.
    /// </summary>
    /// <param name="certDers">The certificates to trust.</param>
    public void TrustCertificates(IEnumerable<ReadOnlyMemory<byte>> certDers);

    /// <summary>
    /// Creates a <see cref="SslStream"/> that trusts the certificates provided
    /// by <see cref="TrustCertificates(IEnumerable{ReadOnlyMemory{byte}})"/>.
    /// </summary>
    /// <param name="innerStream">The stream to wrap.</param>
    public SslStream CreateTlsStream(Stream innerStream);

    /// <summary>
    /// Creates a <see cref="SslStream"/> that trusts all certificates.
    /// </summary>
    /// <param name="innerStream">The stream to wrap.</param>
    public SslStream CreateTlsStreamTrustAll(Stream innerStream);
}
