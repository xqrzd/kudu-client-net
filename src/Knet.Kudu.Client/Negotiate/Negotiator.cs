using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.IO.Pipelines;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Security.Cryptography.X509Certificates;
using System.Security.Principal;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Knet.Kudu.Client.Connection;
using Knet.Kudu.Client.Exceptions;
using Knet.Kudu.Client.Internal;
using Knet.Kudu.Client.Logging;
using Knet.Kudu.Client.Protobuf.Rpc;
using Knet.Kudu.Client.Protobuf.Security;
using Microsoft.Extensions.Logging;
using Pipelines.Sockets.Unofficial;
using static Knet.Kudu.Client.Protobuf.Rpc.ErrorStatusPB.Types;
using static Knet.Kudu.Client.Protobuf.Rpc.NegotiatePB.Types;

namespace Knet.Kudu.Client.Negotiate;

/// <summary>
/// https://github.com/apache/kudu/blob/master/docs/design-docs/rpc.md#negotiation
/// </summary>
public class Negotiator
{
    private const byte CurrentRpcVersion = 9;

    private static readonly ReadOnlyMemory<byte> ConnectionHeader = new byte[]
    {
        (byte)'h', (byte)'r', (byte)'p', (byte)'c',
        CurrentRpcVersion,
        0, // ServiceClass (unused)
        0  // AuthProtocol (unused)
    };

    private static readonly ConnectionContextPB _defaultConnectionContextPb = new();

    private static readonly AuthenticationTypePB _saslAuthnType =
        new() { Sasl = new AuthenticationTypePB.Types.Sasl() };

    private static readonly AuthenticationTypePB _tokenAuthnType =
        new() { Token = new AuthenticationTypePB.Types.Token() };

    private const int ConnectionContextCallId = -3;
    private const int SaslNegotiationCallId = -33;

    private readonly ILoggerFactory _loggerFactory;
    private readonly ILogger _logger;
    private readonly KuduClientOptions _options;
    private readonly ISecurityContext _securityContext;
    private readonly ServerInfo _serverInfo;
    private readonly Socket _socket;

    /// <summary>
    /// The authentication token we'll try to connect with, maybe null.
    /// This is fetched from <see cref="ISecurityContext"/> in the constructor to
    /// ensure that it doesn't change over the course of a negotiation attempt.
    /// </summary>
    private readonly SignedTokenPB? _authnToken;

    private readonly bool _requireAuthentication;
    private readonly bool _requireEncryption;
    private readonly bool _encryptLoopback;

    private Stream _stream;
    private X509Certificate2? _remoteCertificate;
    private string _encryption = "PLAINTEXT";
    private string? _tlsCipher;
    private string _authentication = "SASL/PLAIN";
    private string? _servicePrincipalName;

    public Negotiator(
        KuduClientOptions options,
        ISecurityContext securityContext,
        ILoggerFactory loggerFactory,
        ServerInfo serverInfo,
        Socket socket)
    {
        _loggerFactory = loggerFactory;
        _logger = loggerFactory.CreateLogger<Negotiator>();
        _options = options;
        _securityContext = securityContext;
        _serverInfo = serverInfo;
        _socket = socket;
        _requireAuthentication = options.RequireAuthentication;
        _requireEncryption = options.EncryptionPolicy != EncryptionPolicy.Optional;
        _encryptLoopback = options.EncryptionPolicy == EncryptionPolicy.Required;
        _stream = new NetworkStream(_socket, ownsSocket: false);

        if (securityContext.IsAuthenticationTokenImported)
        {
            // If an authentication token was imported we'll try to use it.
            _authnToken = securityContext.GetAuthenticationToken();
        }
    }

    public async Task<KuduConnection> NegotiateAsync(CancellationToken cancellationToken = default)
    {
        await SendConnectionHeaderAsync(cancellationToken).ConfigureAwait(false);
        var features = await NegotiateFeaturesAsync(cancellationToken).ConfigureAwait(false);

        // Check the negotiated authentication type sent by the server.
        var chosenAuthnType = ChooseAuthenticationType(features);

        var useTls = await NegotiateTlsAsync(features, chosenAuthnType).ConfigureAwait(false);

        var context = await AuthenticateAsync(chosenAuthnType, cancellationToken)
            .ConfigureAwait(false);

        await SendConnectionContextAsync(context, cancellationToken).ConfigureAwait(false);

        _logger.ConnectedToServer(
            _serverInfo.HostPort,
            _serverInfo.Endpoint.Address,
            _encryption,
            _authentication,
            _tlsCipher,
            _servicePrincipalName,
            _serverInfo.IsLocal);

        var pipe = CreateDuplexPipe(useTls);
        return new KuduConnection(pipe, _loggerFactory);
    }

    private ValueTask SendConnectionHeaderAsync(CancellationToken cancellationToken)
    {
        // After the client connects to a server, the client first sends a connection header.
        // The connection header consists of a magic number "hrpc" and three byte flags,
        // for a total of 7 bytes:
        //
        // +----------------------------------+
        // |  "hrpc" 4 bytes                  |
        // +----------------------------------+
        // |  Version (1 byte)                |
        // +----------------------------------+
        // |  ServiceClass (1 byte)           |
        // +----------------------------------+
        // |  AuthProtocol (1 byte)           |
        // +----------------------------------+
        //
        // Currently, the RPC version is 9. The ServiceClass and AuthProtocol fields are unused.
        // https://github.com/apache/kudu/blob/master/docs/design-docs/rpc.md#wire-protocol
        return _stream.WriteAsync(ConnectionHeader, cancellationToken);
    }

    private Task<NegotiatePB> NegotiateFeaturesAsync(CancellationToken cancellationToken)
    {
        // Step 1: Negotiate
        // The client and server swap RPC feature flags, supported authentication types,
        // and supported SASL mechanisms. This step always takes exactly one round trip.

        var request = new NegotiatePB { Step = NegotiateStep.Negotiate };

        // Advertise our supported features
        request.SupportedFeatures.Add(RpcFeatureFlag.ApplicationFeatureFlags);
        request.SupportedFeatures.Add(RpcFeatureFlag.Tls);

        if (_serverInfo.IsLocal && !_encryptLoopback)
            request.SupportedFeatures.Add(RpcFeatureFlag.TlsAuthenticationOnly);

        // Advertise our authentication types.
        // We always advertise SASL.
        request.AuthnTypes.Add(_saslAuthnType);

        // We may also have a token. But, we can only use the token
        // if we are able to use authenticated TLS to authenticate the server.
        if (_authnToken is not null)
        {
            request.AuthnTypes.Add(_tokenAuthnType);
        }

        // Certificate authentication is not supported for external clients.

        return SendReceiveAsync(request, cancellationToken);
    }

    private async Task<bool> NegotiateTlsAsync(NegotiatePB features, AuthenticationType chosenAuthnType)
    {
        var serverFeatures = features.SupportedFeatures;
        var negotiateTls = serverFeatures.Contains(RpcFeatureFlag.Tls);

        if (!negotiateTls && _requireEncryption)
        {
            throw new NonRecoverableException(KuduStatus.NotAuthorized(
                "Server does not support required TLS encryption"));
        }

        // Always use TLS if the server supports it.
        if (negotiateTls)
        {
            // If we negotiated TLS, then we want to start the TLS handshake;
            // otherwise, we can move directly to the authentication phase.
            var tlsStream = await AuthenticateTlsAsync(chosenAuthnType)
                .ConfigureAwait(false);

            // Don't wrap the TLS socket if we are using TLS for authentication only.
            var isAuthOnly = serverFeatures.Contains(RpcFeatureFlag.TlsAuthenticationOnly) &&
                _serverInfo.IsLocal && !_encryptLoopback;

            if (!isAuthOnly)
            {
                _encryption = tlsStream.SslProtocol.ToString();
                _tlsCipher = GetNegotiatedCipherSuite(tlsStream);
                _stream = tlsStream;

                return true;
            }
        }

        return false;
    }

    private async Task<SslStream> AuthenticateTlsAsync(AuthenticationType authenticationType)
    {
        // Step 2: TLS Handshake
        // If both the server and client support the TLS RPC feature flag, the client
        // initiates a TLS handshake, after which both sides wrap the socket in the TLS
        // protected channel. If either the client or server does not support the TLS flag,
        // then this step is skipped. This step takes as many round trips as necessary to
        // complete the TLS handshake.

        var tlsHost = _serverInfo.HostPort.Host;
        var authenticationStream = new KuduTlsAuthenticationStream(this);
        var sslInnerStream = new StreamWrapper(authenticationStream);

        SslStream tlsStream = authenticationType == AuthenticationType.Token ?
            _securityContext.CreateTlsStream(sslInnerStream) :
            _securityContext.CreateTlsStreamTrustAll(sslInnerStream);

        await tlsStream.AuthenticateAsClientAsync(tlsHost).ConfigureAwait(false);

        _remoteCertificate = new X509Certificate2(tlsStream.RemoteCertificate!);

        sslInnerStream.ReplaceStream(_stream);
        return tlsStream;
    }

    private Task<ConnectionContextPB> AuthenticateAsync(
        AuthenticationType authenticationType, CancellationToken cancellationToken)
    {
        // The client and server now authenticate to each other.
        // There are three authentication types (SASL, token, and TLS/PKI certificate),
        // and the SASL authentication type has two possible mechanisms (GSSAPI and PLAIN).
        // Of these, all are considered strong or secure authentication types with the exception
        // of SASL PLAIN.
        // The client sends the server its supported authentication types and sasl
        // mechanisms in the negotiate step above. The server responds in the negotiate step with
        // its preferred authentication type and supported mechanisms. The server is thus responsible
        // for choosing the authentication type if there are multiple to choose from.

        if (authenticationType == AuthenticationType.SaslPlain && _requireAuthentication)
        {
            throw new NonRecoverableException(KuduStatus.NotAuthorized(
                "Client requires authentication, but server does not have Kerberos enabled"));
        }

        return authenticationType switch
        {
            AuthenticationType.SaslGssApi => AuthenticateSaslGssApiAsync(cancellationToken),
            AuthenticationType.SaslPlain => AuthenticateSaslPlainAsync(cancellationToken),
            AuthenticationType.Token => AuthenticateTokenAsync(cancellationToken),
            _ => throw new NonRecoverableException(KuduStatus.IllegalState(
                $"Unsupported authentication type {authenticationType}"))
        };
    }

    private async Task<ConnectionContextPB> AuthenticateSaslPlainAsync(CancellationToken cancellationToken)
    {
        var token = SaslPlain.CreateToken(new NetworkCredential(Environment.UserName, ""));
        var request = new NegotiatePB
        {
            Step = NegotiateStep.SaslInitiate,
            Token = UnsafeByteOperations.UnsafeWrap(token)
        };
        request.SaslMechanisms.Add(new SaslMechanism { Mechanism = "PLAIN" });

        await SendReceiveAsync(request, cancellationToken).ConfigureAwait(false);
        return _defaultConnectionContextPb;
    }

    private async Task<ConnectionContextPB> AuthenticateSaslGssApiAsync(CancellationToken cancellationToken)
    {
        using var gssApiStream = new KuduGssApiAuthenticationStream(this);
        using var negotiateStream = new NegotiateStream(gssApiStream);

        var targetName = $"{_options.SaslProtocolName}/{_serverInfo.HostPort.Host}";
        _authentication = "SASL/GSSAPI Kerberos";
        _servicePrincipalName = targetName;

        // Hopefully a temporary hack-fix, until we can figure
        // out why EncryptAndSign doesn't work on Windows.
        var protectionLevel = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ?
            ProtectionLevel.Sign :
            ProtectionLevel.EncryptAndSign;

        await negotiateStream.AuthenticateAsClientAsync(
            CredentialCache.DefaultNetworkCredentials,
            targetName,
            protectionLevel,
            TokenImpersonationLevel.Identification).ConfigureAwait(false);

        gssApiStream.CompleteNegotiate(negotiateStream);

        var tokenResponse = await SendSaslResponseAsync(Array.Empty<byte>(), cancellationToken)
            .ConfigureAwait(false);

        var token = tokenResponse.Token;
        // NegotiateStream expects a little-endian length header.
        var newToken = new byte[token.Length + 4];
        BinaryPrimitives.WriteInt32LittleEndian(newToken, token.Length);
        token.Span.CopyTo(newToken.AsSpan(4));

        var decryptedToken = gssApiStream.DecryptBuffer(newToken);
        var encryptedToken = gssApiStream.EncryptBuffer(decryptedToken.Span);

        // Remove the little-endian length header added by NegotiateStream.
        encryptedToken = encryptedToken.Slice(4);

        var response = await SendSaslResponseAsync(encryptedToken, cancellationToken)
            .ConfigureAwait(false);

        AssertStep(NegotiateStep.SaslSuccess, response.Step);

        if (_remoteCertificate is not null)
        {
            byte[] expected = _remoteCertificate.GetEndpointChannelBindings();

            if (response.ChannelBindings is null)
            {
                throw new NonRecoverableException(KuduStatus.IllegalState(
                    "No channel bindings provided by remote peer"));
            }

            byte[] provided = response.ChannelBindings.ToByteArray();
            // Kudu supplies a length header in big-endian,
            // but NegotiateStream expects little-endian.
            provided.AsSpan(0, 4).Reverse();

            var unwrapped = gssApiStream.DecryptBuffer(provided);

            if (!unwrapped.Span.SequenceEqual(expected))
            {
                throw new NonRecoverableException(KuduStatus.IllegalState(
                    "Invalid channel bindings provided by remote peer"));
            }
        }

        if (response.Nonce is not null)
        {
            var nonce = gssApiStream.EncryptBuffer(response.Nonce.Span);

            // NegotiateStream writes a little-endian length header,
            // but Kudu is expecting big-endian.
            nonce.Span.Slice(0, 4).Reverse();

            return new ConnectionContextPB { EncodedNonce = UnsafeByteOperations.UnsafeWrap(nonce) };
        }

        return _defaultConnectionContextPb;
    }

    private async Task<ConnectionContextPB> AuthenticateTokenAsync(CancellationToken cancellationToken)
    {
        var request = new NegotiatePB
        {
            Step = NegotiateStep.TokenExchange,
            AuthnToken = _authnToken
        };

        var response = await SendReceiveAsync(request, cancellationToken).ConfigureAwait(false);

        AssertStep(NegotiateStep.TokenExchange, response.Step);
        _authentication = "TOKEN";

        return _defaultConnectionContextPb;
    }

    private Task SendConnectionContextAsync(
        ConnectionContextPB context,
        CancellationToken cancellationToken)
    {
        // Once the SASL negotiation is complete, before the first request, the client sends the
        // server a special call with call_id -3. The body of this call is a ConnectionContextPB.
        // The server should not respond to this call.

        var header = new RequestHeader { CallId = ConnectionContextCallId };
        return SendAsync(header, context, cancellationToken);
    }

    internal Task SendTlsHandshakeAsync(
        ReadOnlyMemory<byte> tlsHandshake,
        CancellationToken cancellationToken)
    {
        var request = new NegotiatePB
        {
            Step = NegotiateStep.TlsHandshake,
            TlsHandshake = UnsafeByteOperations.UnsafeWrap(tlsHandshake)
        };

        return SendAsync(request, cancellationToken);
    }

    internal Task SendSaslInitiateAsync(
        ReadOnlyMemory<byte> saslToken,
        CancellationToken cancellationToken)
    {
        var request = new NegotiatePB
        {
            Step = NegotiateStep.SaslInitiate,
            Token = UnsafeByteOperations.UnsafeWrap(saslToken)
        };

        request.SaslMechanisms.Add(new SaslMechanism { Mechanism = "GSSAPI" });

        return SendAsync(request, cancellationToken);
    }

    internal Task<NegotiatePB> SendSaslResponseAsync(
        ReadOnlyMemory<byte> saslToken,
        CancellationToken cancellationToken)
    {
        var request = new NegotiatePB
        {
            Step = NegotiateStep.SaslResponse,
            Token = UnsafeByteOperations.UnsafeWrap(saslToken)
        };

        return SendReceiveAsync(request, cancellationToken);
    }

    private Task SendAsync(NegotiatePB request, CancellationToken cancellationToken)
    {
        var header = new RequestHeader { CallId = SaslNegotiationCallId };
        return SendAsync(header, request, cancellationToken);
    }

    private async Task<NegotiatePB> SendReceiveAsync(NegotiatePB request, CancellationToken cancellationToken)
    {
        await SendAsync(request, cancellationToken).ConfigureAwait(false);
        var response = await ReceiveResponseAsync(cancellationToken).ConfigureAwait(false);
        return response;
    }

    private async Task SendAsync(RequestHeader header, IMessage message, CancellationToken cancellationToken)
    {
        using var writer = new ArrayPoolBufferWriter<byte>(4096);

        // Make space to write the length of the entire message.
        writer.Advance(4);

        ProtobufHelper.WriteDelimitedTo(header, writer);
        ProtobufHelper.WriteDelimitedTo(message, writer);

        // Go back and write the length of the entire message, minus the 4
        // bytes we already allocated to store the length.
        BinaryPrimitives.WriteInt32BigEndian(writer.WrittenSpan, writer.WrittenCount - 4);

        await _stream.WriteAsync(writer.WrittenMemory, cancellationToken).ConfigureAwait(false);
    }

    internal async Task<NegotiatePB> ReceiveResponseAsync(CancellationToken cancellationToken)
    {
        using var buffer = new ArrayPoolBufferWriter<byte>(4096);

        await ReadExactAsync(4, buffer, cancellationToken).ConfigureAwait(false);

        var messageLength = BinaryPrimitives.ReadInt32BigEndian(buffer.WrittenSpan);
        await ReadExactAsync(messageLength, buffer, cancellationToken).ConfigureAwait(false);

        using var stream = new MemoryStream(buffer.WrittenArray, 4, buffer.WrittenCount - 4);
        return ParseResponse(stream);
    }

    private async Task ReadExactAsync(
        int length,
        ArrayPoolBufferWriter<byte> destination,
        CancellationToken cancellationToken)
    {
        var buffer = destination.GetMemory(length).Slice(0, length);

        do
        {
            var bytesReceived = await _stream.ReadAsync(buffer, cancellationToken)
                .ConfigureAwait(false);

            if (bytesReceived <= 0)
            {
                throw new RecoverableException(
                    KuduStatus.NetworkError("Connection closed during negotiate"));
            }

            buffer = buffer.Slice(bytesReceived);
        } while (buffer.Length > 0);

        destination.Advance(length);
    }

    private IDuplexPipe CreateDuplexPipe(bool useTls)
    {
        if (useTls)
        {
            // Hack-fix for https://github.com/xqrzd/kudu-client-net/issues/44
            var output = PipeWriter.Create(_stream,
                new StreamPipeWriterOptions(leaveOpen: true));

            var input = StreamConnection.GetReader(_stream,
                _options.ReceivePipeOptions);

            return new DuplexPipe(_socket, output, input, _serverInfo.ToString());
        }
        else
        {
            return SocketConnection.Create(_socket,
                _options.SendPipeOptions,
                _options.ReceivePipeOptions,
                name: _serverInfo.ToString());
        }
    }

    private static AuthenticationType ChooseAuthenticationType(NegotiatePB features)
    {
        var authnTypes = features.AuthnTypes;

        if (authnTypes.Count != 1)
        {
            throw new NonRecoverableException(KuduStatus.IllegalState(
                $"Expected server to reply with one authn type, not {authnTypes.Count}"));
        }

        var authType = authnTypes[0];

        if (authType.Sasl is not null)
        {
            var serverMechs = new HashSet<string>();

            foreach (var mech in features.SaslMechanisms)
                serverMechs.Add(mech.Mechanism.ToUpperInvariant());

            if (serverMechs.Contains("GSSAPI"))
                return AuthenticationType.SaslGssApi;

            if (serverMechs.Contains("PLAIN"))
                return AuthenticationType.SaslPlain;

            throw new NonRecoverableException(KuduStatus.IllegalState(
                $"Server supplied unexpected sasl mechanisms {string.Join(",", serverMechs)}"));
        }
        else if (authType.Token is not null)
        {
            return AuthenticationType.Token;
        }
        else
        {
            throw new NonRecoverableException(
                KuduStatus.IllegalState("Server chose bad authn type"));
        }
    }

    private static NegotiatePB ParseResponse(MemoryStream stream)
    {
        var header = ResponseHeader.Parser.ParseDelimitedFrom(stream);

        if (header.IsError)
        {
            var error = ErrorStatusPB.Parser.ParseDelimitedFrom(stream);
            ThrowRpcException(error);
        }

        var callId = header.CallId;
        if (callId != SaslNegotiationCallId)
        {
            ThrowInvalidCallIdException(callId);
        }

        var response = NegotiatePB.Parser.ParseDelimitedFrom(stream);

        return response;
    }

    private static string GetNegotiatedCipherSuite(SslStream tlsStream)
    {
#if NETCOREAPP3_1_OR_GREATER
        return tlsStream.NegotiatedCipherSuite.ToString();
#else
        return "NA";
#endif
    }

    private static void AssertStep(NegotiateStep expected, NegotiateStep step)
    {
        if (step != expected)
        {
            throw new NonRecoverableException(KuduStatus.IllegalState(
                $"Expected NegotiateStep {expected}, received {step}"));
        }
    }

    [DoesNotReturn]
    private static void ThrowInvalidCallIdException(int callId)
    {
        throw new NonRecoverableException(KuduStatus.IllegalState(
            $"Expected CallId {SaslNegotiationCallId}, got {callId}"));
    }

    [DoesNotReturn]
    private static void ThrowRpcException(ErrorStatusPB error)
    {
        var code = error.Code;
        KuduException exception;

        if (code == RpcErrorCodePB.ErrorServerTooBusy ||
            code == RpcErrorCodePB.ErrorUnavailable)
        {
            exception = new RecoverableException(
                KuduStatus.ServiceUnavailable(error.Message));
        }
        else
        {
            exception = new RpcRemoteException(
                KuduStatus.RemoteError(error.Message), error);
        }

        throw exception;
    }

    private enum AuthenticationType
    {
        SaslGssApi,
        SaslPlain,
        Token
    }

    private sealed class DuplexPipe : IDuplexPipe, IDisposable
    {
        private readonly Socket _socket;

        public PipeWriter Output { get; }

        public PipeReader Input { get; }

        public string Name { get; }

        public DuplexPipe(Socket socket, PipeWriter output, PipeReader input, string name)
        {
            _socket = socket;
            Output = output;
            Input = input;
            Name = name;
        }

        public override string ToString() => Name;

        public void Dispose()
        {
            try
            {
                // Try to gracefully close the socket.
                _socket.Shutdown(SocketShutdown.Both);
            }
            catch
            {
                // Ignore any errors since we're tearing down the connection anyway.
            }

            _socket.Dispose();
        }
    }
}
