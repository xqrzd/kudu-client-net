using System;
using System.Buffers.Binary;
using System.Collections.Generic;
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
using Knet.Kudu.Client.Util;
using Microsoft.Extensions.Logging;
using Pipelines.Sockets.Unofficial;
using static Knet.Kudu.Client.Protobuf.Rpc.ErrorStatusPB.Types;
using static Knet.Kudu.Client.Protobuf.Rpc.NegotiatePB.Types;

namespace Knet.Kudu.Client.Negotiate
{
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

        private const int ConnectionContextCallId = -3;
        private const int SaslNegotiationCallId = -33;

        private readonly ILoggerFactory _loggerFactory;
        private readonly ILogger _logger;
        private readonly KuduClientOptions _options;
        private readonly ISecurityContext _securityContext;
        private readonly ServerInfo _serverInfo;
        private readonly Socket _socket;
        private readonly NegotiateLoggerMessage _logMessage;

        /// <summary>
        /// The authentication token we'll try to connect with, maybe null.
        /// This is fetched from <see cref="ISecurityContext"/> in the constructor to
        /// ensure that it doesn't change over the course of a negotiation attempt.
        /// </summary>
        private readonly SignedTokenPB _authnToken;

        private Stream _stream;
        private X509Certificate2 _remoteCertificate;

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
            _logMessage = new NegotiateLoggerMessage();

            if (securityContext.IsAuthenticationTokenImported)
            {
                // If an authentication token was imported we'll try to use it.
                _authnToken = securityContext.GetAuthenticationToken();
            }
        }

        public async Task<KuduConnection> NegotiateAsync(CancellationToken cancellationToken = default)
        {
            var networkStream = new NetworkStream(_socket, ownsSocket: false);
            _stream = networkStream;

            // After the client connects to a server, the client first sends a connection header.
            // The connection header consists of a magic number "hrpc" and three byte flags,
            // for a total of 7 bytes.
            // https://github.com/apache/kudu/blob/master/docs/design-docs/rpc.md#wire-protocol
            await networkStream.WriteAsync(ConnectionHeader, cancellationToken).ConfigureAwait(false);

            var features = await NegotiateFeaturesAsync(cancellationToken).ConfigureAwait(false);

            // Check the negotiated authentication type sent by the server.
            var chosenAuthnType = ChooseAuthenticationType(features);

            var useTls = false;

            // Always use TLS if the server supports it.
            if (features.SupportedFeatures.Contains(RpcFeatureFlag.Tls))
            {
                // If we negotiated TLS, then we want to start the TLS handshake;
                // otherwise, we can move directly to the authentication phase.
                var tlsStream = await NegotiateTlsAsync(networkStream, chosenAuthnType)
                    .ConfigureAwait(false);

                // Don't wrap the TLS socket if we are using TLS for authentication only.
                if (!features.SupportedFeatures.Contains(RpcFeatureFlag.TlsAuthenticationOnly))
                {
                    useTls = true;
                    _stream = tlsStream;
                }
            }

            var context = await AuthenticateAsync(chosenAuthnType, cancellationToken)
                .ConfigureAwait(false);

            await SendConnectionContextAsync(context, cancellationToken).ConfigureAwait(false);

            IDuplexPipe pipe;
            if (useTls)
            {
                // Hack-fix for https://github.com/xqrzd/kudu-client-net/issues/44
                var output = PipeWriter.Create(_stream,
                    new StreamPipeWriterOptions(leaveOpen: true));

                var input = StreamConnection.GetReader(_stream,
                    _options.ReceivePipeOptions);

                pipe = new DuplexPipe(_socket, output, input, _serverInfo.ToString());
            }
            else
            {
                pipe = SocketConnection.Create(_socket,
                    _options.SendPipeOptions,
                    _options.ReceivePipeOptions,
                    name: _serverInfo.ToString());
            }

            _logger.ConnectedToServer(
                _serverInfo.HostPort,
                _serverInfo.Endpoint.Address,
                _logMessage.TlsInfo,
                _logMessage.NegotiateInfo,
                _serverInfo.IsLocal);

            return new KuduConnection(pipe, _loggerFactory);
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

            if (_serverInfo.IsLocal)
                request.SupportedFeatures.Add(RpcFeatureFlag.TlsAuthenticationOnly);

            // Advertise our authentication types.

            // We always advertise SASL.
            request.AuthnTypes.Add(new AuthenticationTypePB { Sasl = new AuthenticationTypePB.Types.Sasl() });

            // We may also have a token. But, we can only use the token
            // if we are able to use authenticated TLS to authenticate the server.
            if (_authnToken != null)
            {
                request.AuthnTypes.Add(new AuthenticationTypePB { Token = new AuthenticationTypePB.Types.Token() });
            }

            // Certificate authentication is not supported for external clients.

            return SendReceiveAsync(request, cancellationToken);
        }

        private async Task<SslStream> NegotiateTlsAsync(
            NetworkStream stream,
            AuthenticationType authenticationType)
        {
            var tlsHost = _serverInfo.HostPort.Host;
            var authenticationStream = new KuduTlsAuthenticationStream(this);
            var sslInnerStream = new StreamWrapper(authenticationStream);

            SslStream tlsStream = authenticationType == AuthenticationType.Token ?
                _securityContext.CreateTlsStream(sslInnerStream) :
                _securityContext.CreateTlsStreamTrustAll(sslInnerStream);

            await tlsStream.AuthenticateAsClientAsync(tlsHost).ConfigureAwait(false);

            _remoteCertificate = new X509Certificate2(tlsStream.RemoteCertificate);
            _logMessage.TlsInfo = GetTlsInfoLogString(tlsStream);

            sslInnerStream.ReplaceStream(stream);
            return tlsStream;
        }

        private static AuthenticationType ChooseAuthenticationType(NegotiatePB features)
        {
            if (features.AuthnTypes.Count != 1)
            {
                throw new NonRecoverableException(KuduStatus.IllegalState(
                    $"Expected server to reply with one authn type, not {features.AuthnTypes.Count}"));
            }

            var authType = features.AuthnTypes[0];

            if (authType.Sasl != null)
            {
                var serverMechs = new HashSet<string>();

                foreach (var mech in features.SaslMechanisms)
                    serverMechs.Add(mech.Mechanism.ToUpper());

                if (serverMechs.Contains("GSSAPI"))
                    return AuthenticationType.SaslGssApi;

                if (serverMechs.Contains("PLAIN"))
                    return AuthenticationType.SaslPlain;

                throw new NonRecoverableException(KuduStatus.IllegalState(
                    $"Server supplied unexpected sasl mechanisms {string.Join(",", serverMechs)}"));
            }
            else if (authType.Token != null)
            {
                return AuthenticationType.Token;
            }
            else
            {
                throw new NonRecoverableException(
                    KuduStatus.IllegalState("Server chose bad authn type"));
            }
        }

        private Task<ConnectionContextPB> AuthenticateAsync(
            AuthenticationType authenticationType, CancellationToken cancellationToken)
        {
            // The client and server now authenticate to each other.
            // There are three authentication types (SASL, token, and TLS/PKI certificate),
            // and the SASL authentication type has two possible mechanisms (GSSAPI and PLAIN).
            // Of these, all are considered strong or secure authentication types with the exception of SASL PLAIN.
            // The client sends the server its supported authentication types and sasl mechanisms in the negotiate step above.
            // The server responds in the negotiate step with its preferred authentication type and supported mechanisms.
            // The server is thus responsible for choosing the authentication type if there are multiple to choose from.
            // Which type is chosen for a particular connection by the server depends on configuration and the available credentials:

            return authenticationType switch
            {
                AuthenticationType.SaslGssApi => AuthenticateSaslGssApiAsync(cancellationToken),
                AuthenticationType.SaslPlain => AuthenticateSaslPlainAsync(cancellationToken),
                AuthenticationType.Token => AuthenticateTokenAsync(cancellationToken),

                _ => throw new Exception($"Unsupported authentication type {authenticationType}"),
            };
        }

        private async Task<ConnectionContextPB> AuthenticateSaslPlainAsync(CancellationToken cancellationToken)
        {
            var request = new NegotiatePB { Step = NegotiateStep.SaslInitiate };
            request.SaslMechanisms.Add(new SaslMechanism { Mechanism = "PLAIN" });
            var token = SaslPlain.CreateToken(new NetworkCredential(Environment.UserName, ""));
            request.Token = UnsafeByteOperations.UnsafeWrap(token);

            await SendReceiveAsync(request, cancellationToken).ConfigureAwait(false);
            return new ConnectionContextPB();
        }

        private async Task<ConnectionContextPB> AuthenticateSaslGssApiAsync(CancellationToken cancellationToken)
        {
            using var gssApiStream = new KuduGssApiAuthenticationStream(this);
            using var negotiateStream = new NegotiateStream(gssApiStream);

            var targetName = $"{_options.SaslProtocolName}/{_serverInfo.HostPort.Host}";
            _logMessage.NegotiateInfo = $"SASL_GSSAPI [{targetName}]";

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

            var tokenResponse = await SendGssApiTokenAsync(
                NegotiateStep.SaslResponse,
                Array.Empty<byte>(),
                cancellationToken).ConfigureAwait(false);

            var token = tokenResponse.Token;
            // NegotiateStream expects a little-endian length header.
            var newToken = new byte[token.Length + 4];
            BinaryPrimitives.WriteInt32LittleEndian(newToken, token.Length);
            token.Memory.Span.CopyTo(newToken.AsSpan(4));

            var decryptedToken = gssApiStream.DecryptBuffer(newToken);
            var encryptedToken = gssApiStream.EncryptBuffer(decryptedToken);

            // Remove the little-endian length header added by NegotiateStream.
            encryptedToken = encryptedToken.Slice(4);

            var response = await SendGssApiTokenAsync(
                NegotiateStep.SaslResponse,
                encryptedToken,
                cancellationToken).ConfigureAwait(false);

            ExpectStep(NegotiateStep.SaslSuccess, response.Step);

            if (_remoteCertificate != null)
            {
                byte[] expected = _remoteCertificate.GetEndpointChannelBindings();

                if (response.ChannelBindings == null)
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

            byte[] nonce = null;

            if (response.Nonce != null)
            {
                nonce = gssApiStream.EncryptBuffer(response.Nonce.Memory).ToArray();

                // NegotiateStream writes a little-endian length header,
                // but Kudu is expecting big-endian.
                nonce.AsSpan(0, 4).Reverse();
            }

            return new ConnectionContextPB { EncodedNonce = UnsafeByteOperations.UnsafeWrap(nonce) };
        }

        private async Task<ConnectionContextPB> AuthenticateTokenAsync(CancellationToken cancellationToken)
        {
            var request = new NegotiatePB { Step = NegotiateStep.TokenExchange };
            request.AuthnToken = _authnToken;

            _logMessage.NegotiateInfo = "TOKEN_AUTH";

            var response = await SendReceiveAsync(request, cancellationToken).ConfigureAwait(false);

            ExpectStep(NegotiateStep.TokenExchange, response.Step);

            return new ConnectionContextPB();
        }

        private Task SendConnectionContextAsync(ConnectionContextPB context, CancellationToken cancellationToken)
        {
            // Once the SASL negotiation is complete, before the first request, the client sends the
            // server a special call with call_id -3. The body of this call is a ConnectionContextPB.
            // The server should not respond to this call.

            var header = new RequestHeader { CallId = ConnectionContextCallId };

            return SendAsync(header, context, cancellationToken);
        }

        internal Task SendTlsHandshakeAsync(ReadOnlyMemory<byte> tlsHandshake, CancellationToken cancellationToken)
        {
            var request = new NegotiatePB
            {
                Step = NegotiateStep.TlsHandshake,
                TlsHandshake = UnsafeByteOperations.UnsafeWrap(tlsHandshake)
            };

            return SendAsync(request, cancellationToken);
        }

        internal Task<NegotiatePB> ReceiveTlsHandshakeAsync(CancellationToken cancellationToken)
        {
            return ReceiveAsync(cancellationToken);
        }

        internal Task<NegotiatePB> SendGssApiTokenAsync(
            NegotiateStep step, ReadOnlyMemory<byte> saslToken, CancellationToken cancellationToken)
        {
            var request = new NegotiatePB
            {
                Step = step,
                Token = UnsafeByteOperations.UnsafeWrap(saslToken)
            };

            if (step == NegotiateStep.SaslInitiate)
            {
                request.SaslMechanisms.Add(new SaslMechanism { Mechanism = "GSSAPI" });
            }

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
            var response = await ReceiveAsync(cancellationToken).ConfigureAwait(false);
            return response;
        }

        private async Task SendAsync<TInput>(RequestHeader header, TInput body, CancellationToken cancellationToken)
            where TInput : IMessage
        {
            using var stream = new RecyclableMemoryStream();

            // Make space to write the length of the entire message.
            stream.GetMemory(4);

            header.WriteDelimitedTo(stream);
            body.WriteDelimitedTo(stream);

            //Serializer.SerializeWithLengthPrefix(stream, header, PrefixStyle.Base128);
            //Serializer.SerializeWithLengthPrefix(stream, body, PrefixStyle.Base128);

            // Go back and write the length of the entire message, minus the 4
            // bytes we already allocated to store the length.
            BinaryPrimitives.WriteUInt32BigEndian(stream.AsSpan(), (uint)stream.Length - 4);

            await _stream.WriteAsync(stream.AsMemory(), cancellationToken).ConfigureAwait(false);
        }

        private async Task<NegotiatePB> ReceiveAsync(CancellationToken cancellationToken)
        {
            var buffer = new byte[4];
            await ReadExactAsync(buffer, cancellationToken).ConfigureAwait(false);
            var messageLength = BinaryPrimitives.ReadInt32BigEndian(buffer);

            buffer = new byte[messageLength];
            await ReadExactAsync(buffer, cancellationToken).ConfigureAwait(false);

            var ms = new MemoryStream(buffer);
            var header = ResponseHeader.Parser.ParseDelimitedFrom(ms);

            if (header.IsError)
            {
                var error = ErrorStatusPB.Parser.ParseDelimitedFrom(ms);
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

            if (header.CallId != SaslNegotiationCallId)
            {
                throw new NonRecoverableException(KuduStatus.IllegalState(
                    $"Expected CallId {SaslNegotiationCallId}, got {header.CallId}"));
            }

            var response = NegotiatePB.Parser.ParseDelimitedFrom(ms);

            return response;
        }

        private async Task ReadExactAsync(Memory<byte> buffer, CancellationToken cancellationToken)
        {
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
        }

        private static string GetTlsInfoLogString(SslStream tlsStream)
        {
#if NETCOREAPP3_1_OR_GREATER
            return $"{tlsStream.SslProtocol.ToString().ToUpper()} [{tlsStream.NegotiatedCipherSuite}]";
#else
            return $"{tlsStream.SslProtocol.ToString().ToUpper()}";
#endif
        }

        private static void ExpectStep(NegotiateStep expected, NegotiateStep step)
        {
            if (step != expected)
            {
                throw new NonRecoverableException(KuduStatus.IllegalState(
                    $"Expected NegotiateStep {expected}, received {step}"));
            }
        }

        private enum AuthenticationType
        {
            SaslGssApi,
            SaslPlain,
            Token
        }

        private class NegotiateLoggerMessage
        {
            public string TlsInfo { get; set; } = "PLAINTEXT";

            public string NegotiateInfo { get; set; } = "SASL_PLAIN";
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
                _socket.Shutdown(SocketShutdown.Both);
                _socket.Close();
                _socket.Dispose();
            }
        }
    }
}
