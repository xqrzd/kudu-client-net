using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.IO.Pipelines;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using System.Security.Principal;
using System.Threading;
using System.Threading.Tasks;
using Knet.Kudu.Client.Connection;
using Knet.Kudu.Client.Exceptions;
using Knet.Kudu.Client.Internal;
using Knet.Kudu.Client.Logging;
using Knet.Kudu.Client.Protocol.Rpc;
using Knet.Kudu.Client.Util;
using Microsoft.Extensions.Logging;
using Pipelines.Sockets.Unofficial;
using ProtoBuf;
using static Knet.Kudu.Client.Protocol.Rpc.NegotiatePB;

namespace Knet.Kudu.Client.Negotiate
{
    /// <summary>
    /// https://github.com/apache/kudu/blob/master/docs/design-docs/rpc.md#negotiation
    /// </summary>
    public class Negotiator
    {
        // TODO: Rework this class, and support more negotiation methods.
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

        private readonly ILogger _logger;
        private readonly KuduClientOptions _options;
        private readonly ServerInfo _serverInfo;
        private readonly Socket _socket;
        private readonly NegotiateLoggerMessage _logMessage;

        private Stream _stream;
        private X509Certificate2 _remoteCertificate;

        public Negotiator(
            KuduClientOptions options,
            ILoggerFactory loggerFactory,
            ServerInfo serverInfo,
            Socket socket)
        {
            _logger = loggerFactory.CreateLogger<Negotiator>();
            _options = options;
            _serverInfo = serverInfo;
            _socket = socket;
            _logMessage = new NegotiateLoggerMessage();
        }

        public async Task<KuduConnection> NegotiateAsync(CancellationToken cancellationToken = default)
        {
            var networkStream = new NetworkStream(_socket, ownsSocket: false);
            _stream = networkStream;

            // After the client connects to a server, the client first sends a connection header.
            // The connection header consists of a magic number "hrpc" and three byte flags, for a total of 7 bytes.
            // https://github.com/apache/kudu/blob/master/docs/design-docs/rpc.md#wire-protocol
            await networkStream.WriteAsync(ConnectionHeader, cancellationToken).ConfigureAwait(false);

            var features = await NegotiateFeaturesAsync(cancellationToken).ConfigureAwait(false);
            var useTls = false;

            // Always use TLS if the server supports it.
            if (features.HasRpcFeature(RpcFeatureFlag.Tls))
            {
                var tlsHost = _options.TlsHost ?? _serverInfo.HostPort.Host;

                // If we negotiated TLS, then we want to start the TLS handshake;
                // otherwise, we can move directly to the authentication phase.
                var tlsStream = await NegotiateTlsAsync(networkStream, tlsHost).ConfigureAwait(false);

                // Don't wrap the TLS socket if we are using TLS for authentication only.
                if (!features.HasRpcFeature(RpcFeatureFlag.TlsAuthenticationOnly))
                {
                    useTls = true;
                    _stream = tlsStream;
                }
            }

            // Check the negotiated authentication type sent by the server.
            var chosenAuthnType = ChooseAuthenticationType(features);

            var result = await AuthenticateAsync(chosenAuthnType, cancellationToken)
                .ConfigureAwait(false);

            await SendConnectionContextAsync(result.Nonce, cancellationToken).ConfigureAwait(false);

            IDuplexPipe pipe;
            if (useTls)
            {
                pipe = StreamConnection.GetDuplex(_stream,
                    _options.SendPipeOptions,
                    _options.ReceivePipeOptions,
                    name: _serverInfo.ToString());
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

            var socketConnection = new KuduSocketConnection(_socket, pipe);
            return new KuduConnection(socketConnection);
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
            request.AuthnTypes.Add(new AuthenticationTypePB { sasl = new AuthenticationTypePB.Sasl() });

            // TODO: Advertise token authentication if we have a token.
            // TODO: Advertise certificate authentication if we have a certificate.

            return SendReceiveAsync(request, cancellationToken);
        }

        private async Task<SslStream> NegotiateTlsAsync(NetworkStream stream, string tlsHost)
        {
            var authenticationStream = new KuduTlsAuthenticationStream(this);
            var sslInnerStream = new StreamWrapper(authenticationStream);
            var tlsStream = SslStreamFactory.CreateSslStreamTrustAll(sslInnerStream);
            await tlsStream.AuthenticateAsClientAsync(tlsHost).ConfigureAwait(false);

            _remoteCertificate = new X509Certificate2(tlsStream.RemoteCertificate);
            _logMessage.TlsInfo = GetTlsInfoLogString(tlsStream);

            sslInnerStream.ReplaceStream(stream);
            return tlsStream;
        }

        private AuthenticationType ChooseAuthenticationType(NegotiatePB features)
        {
            if (features.AuthnTypes.Count != 1)
            {
                throw new NonRecoverableException(KuduStatus.IllegalState(
                    $"Expected server to reply with one authn type, not {features.AuthnTypes.Count}"));
            }

            var authType = features.AuthnTypes[0];

            if (authType.sasl != null)
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
            else if (authType.certificate != null)
            {
                return AuthenticationType.Certificate;
            }
            else if (authType.token != null)
            {
                return AuthenticationType.Token;
            }
            else
            {
                throw new NonRecoverableException(
                    KuduStatus.IllegalState("Server chose bad authn type"));
            }
        }

        private Task<AuthenticationResult> AuthenticateAsync(
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

                _ => throw new Exception($"Unsupported authentication type {authenticationType}"),
            };
        }

        private async Task<AuthenticationResult> AuthenticateSaslPlainAsync(CancellationToken cancellationToken)
        {
            var request = new NegotiatePB { Step = NegotiateStep.SaslInitiate };
            request.SaslMechanisms.Add(new SaslMechanism { Mechanism = "PLAIN" });
            request.Token = SaslPlain.CreateToken(new NetworkCredential("demo", "demo"));

            await SendReceiveAsync(request, cancellationToken).ConfigureAwait(false);
            return new AuthenticationResult();
        }

        private async Task<AuthenticationResult> AuthenticateSaslGssApiAsync(CancellationToken cancellationToken)
        {
            using var gssApiStream = new KuduGssApiAuthenticationStream(this);
            using var negotiateStream = new NegotiateStream(gssApiStream);

            var targetName = _options.KerberosSpn ?? $"kudu/{_serverInfo.HostPort.Host}";
            _logMessage.NegotiateInfo = $"SaslGssApi [{targetName}]";

            await negotiateStream.AuthenticateAsClientAsync(
                CredentialCache.DefaultNetworkCredentials,
                targetName,
                ProtectionLevel.EncryptAndSign,
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
            token.CopyTo(newToken.AsSpan(4));

            var decryptedToken = gssApiStream.DecryptBuffer(newToken);
            var encryptedToken = gssApiStream.EncryptBuffer(decryptedToken);

            // Remove the little-endian length header added by NegotiateStream.
            encryptedToken = encryptedToken.Slice(4);

            var response = await SendGssApiTokenAsync(
                NegotiateStep.SaslResponse,
                encryptedToken,
                cancellationToken).ConfigureAwait(false);

            if (response.Step != NegotiateStep.SaslSuccess)
                throw new Exception($"Negotiate failed, expected step {NegotiateStep.SaslSuccess}, got {response.Step}");

            if (_remoteCertificate != null)
            {
                byte[] expected = _remoteCertificate.GetEndpointChannelBindings();

                if (response.ChannelBindings == null)
                    throw new Exception("No channel bindings provided by remote peer");

                byte[] provided = response.ChannelBindings;
                // Kudu supplies a length header in big-endian,
                // but NegotiateStream expects little-endian.
                provided.AsSpan(0, 4).Reverse();

                var unwrapped = gssApiStream.DecryptBuffer(provided);

                if (!unwrapped.Span.SequenceEqual(expected))
                    throw new Exception("Invalid channel bindings provided by remote peer");
            }

            byte[] nonce = null;

            if (response.Nonce != null)
            {
                nonce = gssApiStream.EncryptBuffer(response.Nonce).ToArray();

                // NegotiateStream writes a little-endian length header,
                // but Kudu is expecting big-endian.
                nonce.AsSpan(0, 4).Reverse();
            }

            return new AuthenticationResult(nonce);
        }

        private Task SendConnectionContextAsync(byte[] nonce, CancellationToken cancellationToken)
        {
            // Once the SASL negotiation is complete, before the first request, the client sends the
            // server a special call with call_id -3. The body of this call is a ConnectionContextPB.
            // The server should not respond to this call.

            var header = new RequestHeader { CallId = ConnectionContextCallId };
            var request = new ConnectionContextPB { EncodedNonce = nonce };

            return SendAsync(header, request, cancellationToken);
        }

        internal Task<NegotiatePB> SendTlsHandshakeAsync(byte[] tlsHandshake, CancellationToken cancellationToken)
        {
            var request = new NegotiatePB
            {
                Step = NegotiateStep.TlsHandshake,
                TlsHandshake = tlsHandshake
            };

            return SendReceiveAsync(request, cancellationToken);
        }

        internal Task<NegotiatePB> SendGssApiTokenAsync(
            NegotiateStep step, ReadOnlyMemory<byte> saslToken, CancellationToken cancellationToken)
        {
            var request = new NegotiatePB
            {
                Step = step,
                Token = saslToken.ToArray()
            };

            if (step == NegotiateStep.SaslInitiate)
            {
                request.SaslMechanisms.Add(new SaslMechanism { Mechanism = "GSSAPI" });
            }

            return SendReceiveAsync(request, cancellationToken);
        }

        private async Task<NegotiatePB> SendReceiveAsync(NegotiatePB request, CancellationToken cancellationToken)
        {
            var header = new RequestHeader { CallId = SaslNegotiationCallId };
            await SendAsync(header, request, cancellationToken).ConfigureAwait(false);
            var response = await ReceiveAsync(cancellationToken).ConfigureAwait(false);
            return response;
        }

        private async Task SendAsync<TInput>(RequestHeader header, TInput body, CancellationToken cancellationToken)
        {
            using var stream = new RecyclableMemoryStream();

            // Make space to write the length of the entire message.
            stream.GetMemory(4);

            Serializer.SerializeWithLengthPrefix(stream, header, PrefixStyle.Base128);
            Serializer.SerializeWithLengthPrefix(stream, body, PrefixStyle.Base128);

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
            var header = Serializer.DeserializeWithLengthPrefix<ResponseHeader>(ms, PrefixStyle.Base128);

            if (header.CallId != SaslNegotiationCallId)
                throw new Exception($"Negotiate failed, expected {SaslNegotiationCallId}, got {header.CallId}");
            var response = Serializer.DeserializeWithLengthPrefix<NegotiatePB>(ms, PrefixStyle.Base128);

            return response;
        }

        private async Task ReadExactAsync(Memory<byte> buffer, CancellationToken cancellationToken)
        {
            do
            {
                var read = await _stream.ReadAsync(buffer, cancellationToken).ConfigureAwait(false);
                buffer = buffer.Slice(read);
            } while (buffer.Length > 0);
        }

        private static string GetTlsInfoLogString(SslStream tlsStream)
        {
#if NETCOREAPP3_0
            return $"{tlsStream.SslProtocol} [{tlsStream.NegotiatedCipherSuite}]";
#else
            return $"{tlsStream.SslProtocol}";
#endif
        }

        private enum AuthenticationType
        {
            SaslGssApi,
            SaslPlain,
            Token,
            Certificate
        }

        private readonly struct AuthenticationResult
        {
            public byte[] Nonce { get; }

            public AuthenticationResult(byte[] nonce)
            {
                Nonce = nonce;
            }
        }

        private class NegotiateLoggerMessage
        {
            public string TlsInfo { get; set; } = "Plaintext";

            public string NegotiateInfo { get; set; } = "SaslPlain";
        }
    }
}
