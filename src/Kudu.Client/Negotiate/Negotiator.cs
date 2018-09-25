using System.Diagnostics;
using System.Net;
using System.Threading.Tasks;
using Kudu.Client.Connection;
using Kudu.Client.Protocol.Rpc;

namespace Kudu.Client.Negotiate
{
    /// <summary>
    /// https://github.com/apache/kudu/blob/master/docs/design-docs/rpc.md#negotiation
    /// </summary>
    public class Negotiator
    {
        // TODO: Rework this class, and support more negotiation methods.
        private const int ConnectionContextCallID = -3;
        private const int SASLNegotiationCallID = -33;

        private readonly KuduConnection _client;

        public Negotiator(KuduConnection client)
        {
            _client = client;
        }

        public async Task NegotiateAsync()
        {
            var features = await NegotiateFeaturesAsync();
            var result = await AuthenticateAsync();
            Debug.Assert(result.Step == NegotiatePB.NegotiateStep.SaslSuccess);
            await SendConnectionContextAsync();
        }

        private async Task<NegotiatePB> NegotiateFeaturesAsync()
        {
            // Step 1: Negotiate
            // The client and server swap RPC feature flags, supported authentication types,
            // and supported SASL mechanisms. This step always takes exactly one round trip.

            var header = new RequestHeader { CallId = SASLNegotiationCallID };
            var request = new NegotiatePB { Step = NegotiatePB.NegotiateStep.Negotiate };
            request.SupportedFeatures.Add(RpcFeatureFlag.ApplicationFeatureFlags);
            request.SaslMechanisms.Add(new NegotiatePB.SaslMechanism { Mechanism = "PLAIN" });

            using (var response = await _client.SendReceiveAsync(header, request))
            {
                return response.ParseResponse<NegotiatePB>();
            }
        }

        private async Task<NegotiatePB> AuthenticateAsync()
        {
            // The client and server now authenticate to each other.
            // There are three authentication types (SASL, token, and TLS/PKI certificate),
            // and the SASL authentication type has two possible mechanisms (GSSAPI and PLAIN).
            // Of these, all are considered strong or secure authentication types with the exception of SASL PLAIN.
            // The client sends the server its supported authentication types and sasl mechanisms in the negotiate step above.
            // The server responds in the negotiate step with its preferred authentication type and supported mechanisms.
            // The server is thus responsible for choosing the authentication type if there are multiple to choose from.
            // Which type is chosen for a particular connection by the server depends on configuration and the available credentials:

            var header = new RequestHeader { CallId = SASLNegotiationCallID };
            var request = new NegotiatePB { Step = NegotiatePB.NegotiateStep.SaslInitiate };
            request.SaslMechanisms.Add(new NegotiatePB.SaslMechanism { Mechanism = "PLAIN" });
            request.Token = SaslPlain.CreateToken(new NetworkCredential("demo", "demo"));

            using (var response = await _client.SendReceiveAsync(header, request))
            {
                return response.ParseResponse<NegotiatePB>();
            }
        }

        private ValueTask SendConnectionContextAsync()
        {
            // Once the SASL negotiation is complete, before the first request, the client sends the
            // server a special call with call_id -3. The body of this call is a ConnectionContextPB.
            // The server should not respond to this call.

            var connectionHeader = new RequestHeader { CallId = ConnectionContextCallID };
            var connection = new ConnectionContextPB();

            return _client.WriteAsync(connectionHeader, connection);
        }
    }
}
