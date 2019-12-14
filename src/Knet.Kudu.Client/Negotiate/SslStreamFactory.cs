using System.IO;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;

namespace Knet.Kudu.Client.Negotiate
{
    public static class SslStreamFactory
    {
        public static SslStream CreateSslStreamTrustAll(Stream innerStream) =>
            new SslStream(innerStream, leaveInnerStreamOpen: true, AllowAnyCertificate);

        private static bool AllowAnyCertificate(object sender,
            X509Certificate certificate, X509Chain chain, SslPolicyErrors sslPolicyErrors) => true;
    }
}
