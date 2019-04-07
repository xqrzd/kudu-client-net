using System;
using System.Buffers.Binary;
using System.Diagnostics;
using System.IO;
using System.IO.Pipelines;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Threading.Tasks;
using Kudu.Client.Connection;
using Kudu.Client.Internal;
using Kudu.Client.Protocol.Rpc;
using Kudu.Client.Util;
using Pipelines.Sockets.Unofficial;
using ProtoBuf;

namespace Kudu.Client.Negotiate
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

        private const int ConnectionContextCallID = -3;
        private const int SASLNegotiationCallID = -33;

        private readonly ServerInfo _serverInfo;
        private readonly Socket _socket;
        private readonly PipeOptions _sendPipeOptions;
        private readonly PipeOptions _receivePipeOptions;

        private Stream _stream;

        public Negotiator(ServerInfo serverInfo, Socket socket,
            PipeOptions sendPipeOptions, PipeOptions receivePipeOptions)
        {
            _serverInfo = serverInfo;
            _socket = socket;
            _sendPipeOptions = sendPipeOptions;
            _receivePipeOptions = receivePipeOptions;
        }

        public async Task<KuduConnection> NegotiateAsync()
        {
            var networkStream = new NetworkStream(_socket, ownsSocket: false);
            _stream = networkStream;

            // After the client connects to a server, the client first sends a connection header.
            // The connection header consists of a magic number "hrpc" and three byte flags, for a total of 7 bytes.
            // https://github.com/apache/kudu/blob/master/docs/design-docs/rpc.md#wire-protocol
            await networkStream.WriteAsync(ConnectionHeader).ConfigureAwait(false);

            var features = await NegotiateFeaturesAsync().ConfigureAwait(false);
            var useTls = false;

            // Always use TLS if the server supports it.
            if (features.HasRpcFeature(RpcFeatureFlag.Tls))
            {
                // TODO: Allow user to supply this in.
                var tlsHost = _serverInfo.HostPort.Host;

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

            var result = await AuthenticateAsync().ConfigureAwait(false);

            Debug.Assert(result.Step == NegotiatePB.NegotiateStep.SaslSuccess);

            await SendConnectionContextAsync().ConfigureAwait(false);

            IDuplexPipe pipe;
            if (useTls)
            {
                pipe = StreamConnection.GetDuplex(_stream,
                    _sendPipeOptions, _receivePipeOptions,
                    name: _serverInfo.ToString());
            }
            else
            {
                pipe = SocketConnection.Create(_socket,
                    _sendPipeOptions, _receivePipeOptions,
                    name: _serverInfo.ToString());
            }

            var socketConnection = new KuduSocketConnection(_socket, pipe);
            return new KuduConnection(socketConnection);
        }

        private Task<NegotiatePB> NegotiateFeaturesAsync()
        {
            // Step 1: Negotiate
            // The client and server swap RPC feature flags, supported authentication types,
            // and supported SASL mechanisms. This step always takes exactly one round trip.

            var request = new NegotiatePB { Step = NegotiatePB.NegotiateStep.Negotiate };
            request.SupportedFeatures.Add(RpcFeatureFlag.ApplicationFeatureFlags);
            request.SupportedFeatures.Add(RpcFeatureFlag.Tls);
            if (_serverInfo.IsLocal)
                request.SupportedFeatures.Add(RpcFeatureFlag.TlsAuthenticationOnly);
            request.SaslMechanisms.Add(new NegotiatePB.SaslMechanism { Mechanism = "PLAIN" });

            return SendReceiveAsync(request);
        }

        private async Task<SslStream> NegotiateTlsAsync(NetworkStream stream, string tlsHost)
        {
            var authenticationStream = new KuduTlsAuthenticationStream(this);
            var sslInnerStream = new StreamWrapper(authenticationStream);
            var sslStream = SslStreamFactory.CreateSslStreamTrustAll(sslInnerStream);
            await sslStream.AuthenticateAsClientAsync(tlsHost).ConfigureAwait(false);

            Console.WriteLine($"TLS Connected {tlsHost}");

            sslInnerStream.ReplaceStream(stream);
            return sslStream;
        }

        private Task<NegotiatePB> AuthenticateAsync()
        {
            // The client and server now authenticate to each other.
            // There are three authentication types (SASL, token, and TLS/PKI certificate),
            // and the SASL authentication type has two possible mechanisms (GSSAPI and PLAIN).
            // Of these, all are considered strong or secure authentication types with the exception of SASL PLAIN.
            // The client sends the server its supported authentication types and sasl mechanisms in the negotiate step above.
            // The server responds in the negotiate step with its preferred authentication type and supported mechanisms.
            // The server is thus responsible for choosing the authentication type if there are multiple to choose from.
            // Which type is chosen for a particular connection by the server depends on configuration and the available credentials:

            var request = new NegotiatePB { Step = NegotiatePB.NegotiateStep.SaslInitiate };
            request.SaslMechanisms.Add(new NegotiatePB.SaslMechanism { Mechanism = "PLAIN" });
            request.Token = SaslPlain.CreateToken(new NetworkCredential("demo", "demo"));

            return SendReceiveAsync(request);
        }

        private Task SendConnectionContextAsync()
        {
            // Once the SASL negotiation is complete, before the first request, the client sends the
            // server a special call with call_id -3. The body of this call is a ConnectionContextPB.
            // The server should not respond to this call.

            var header = new RequestHeader { CallId = ConnectionContextCallID };
            var request = new ConnectionContextPB();

            return SendAsync(header, request);
        }

        internal Task<NegotiatePB> SendTlsHandshakeAsync(byte[] tlsHandshake)
        {
            var request = new NegotiatePB
            {
                Step = NegotiatePB.NegotiateStep.TlsHandshake,
                TlsHandshake = tlsHandshake
            };

            return SendReceiveAsync(request);
        }

        private async Task<NegotiatePB> SendReceiveAsync(NegotiatePB request)
        {
            var header = new RequestHeader { CallId = SASLNegotiationCallID };
            await SendAsync(header, request).ConfigureAwait(false);
            var response = await ReceiveAsync().ConfigureAwait(false);
            return response;
        }

        private async Task SendAsync<TInput>(RequestHeader header, TInput body)
        {
            using (var stream = new RecyclableMemoryStream())
            {
                // Make space to write the length of the entire message.
                stream.GetMemory(4);

                Serializer.SerializeWithLengthPrefix(stream, header, PrefixStyle.Base128);
                Serializer.SerializeWithLengthPrefix(stream, body, PrefixStyle.Base128);

                // Go back and write the length of the entire message, minus the 4
                // bytes we already allocated to store the length.
                BinaryPrimitives.WriteUInt32BigEndian(stream.AsSpan(), (uint)stream.Length - 4);

                await _stream.WriteAsync(stream.AsMemory()).ConfigureAwait(false);
            }
        }

        private async Task<NegotiatePB> ReceiveAsync()
        {
            var buffer = new byte[4];
            await ReadExactAsync(buffer).ConfigureAwait(false);
            var messageLength = BinaryPrimitives.ReadInt32BigEndian(buffer);

            buffer = new byte[messageLength];
            await ReadExactAsync(buffer).ConfigureAwait(false);

            var ms = new MemoryStream(buffer);
            var header = Serializer.DeserializeWithLengthPrefix<ResponseHeader>(ms, PrefixStyle.Base128);
            if (header.CallId != SASLNegotiationCallID)
                throw new Exception($"Negotiate failed, expected {SASLNegotiationCallID}, got {header.CallId}");
            var response = Serializer.DeserializeWithLengthPrefix<NegotiatePB>(ms, PrefixStyle.Base128);

            return response;
        }

        private async Task ReadExactAsync(Memory<byte> buffer)
        {
            do
            {
                var read = await _stream.ReadAsync(buffer).ConfigureAwait(false);
                buffer = buffer.Slice(read);
            } while (buffer.Length > 0);
        }
    }
}
