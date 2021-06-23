using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using Google.Protobuf;
using Knet.Kudu.Client.Protobuf.Client;
using Knet.Kudu.Client.Protobuf.Security;
using Knet.Kudu.Client.Util;

namespace Knet.Kudu.Client.Connection
{
    /// <inheritdoc cref="ISecurityContext" />
    public class SecurityContext : ISecurityContext
    {
        private readonly object _lockObj = new();

        private SignedTokenPB _authnToken;
        private List<X509Certificate2> _trustedCertificates;
        private bool _isAuthenticationTokenImported;

        public bool IsAuthenticationTokenImported
        {
            get
            {
                lock (_lockObj)
                {
                    return _isAuthenticationTokenImported;
                }
            }
        }

        public void SetAuthenticationToken(SignedTokenPB token)
        {
            lock (_lockObj)
            {
                _authnToken = token;
            }
        }

        public SignedTokenPB GetAuthenticationToken()
        {
            lock (_lockObj)
            {
                return _authnToken;
            }
        }

        public ReadOnlyMemory<byte> ExportAuthenticationCredentials()
        {
            var tokenPb = new AuthenticationCredentialsPB();

            lock (_lockObj)
            {
                tokenPb.AuthnToken = _authnToken;

                foreach (var certificate in _trustedCertificates)
                {
                    tokenPb.CaCertDers.Add(UnsafeByteOperations.UnsafeWrap(certificate.RawData));
                }
            }

            var writer = new ArrayBufferWriter<byte>();
            tokenPb.WriteTo(writer);

            return writer.WrittenMemory;
        }

        public void ImportAuthenticationCredentials(ReadOnlyMemory<byte> token)
        {
            // TODO: Use span overload when Google.Protobuf 3.18 is released.
            var tokenPb = AuthenticationCredentialsPB.Parser.ParseFrom(token.ToArray());
            var caCertDers = tokenPb.CaCertDers.ToMemoryArray();

            lock (_lockObj)
            {
                if (tokenPb.AuthnToken != null)
                {
                    _authnToken = tokenPb.AuthnToken;
                }

                TrustCertificates(caCertDers);

                _isAuthenticationTokenImported = true;
            }
        }

        public void TrustCertificates(IEnumerable<ReadOnlyMemory<byte>> certDers)
        {
            var certificates = new List<X509Certificate2>();

            foreach (var cert in certDers)
            {
#if NET5_0_OR_GREATER
                certificates.Add(new X509Certificate2(cert.Span));
#else
                certificates.Add(new X509Certificate2(cert.ToArray()));
#endif
            }

            lock (_lockObj)
            {
                _trustedCertificates = certificates;
            }
        }

        public SslStream CreateTlsStream(Stream innerStream) =>
            new(innerStream, leaveInnerStreamOpen: true, ValidateCertificate);

        public SslStream CreateTlsStreamTrustAll(Stream innerStream) =>
            new(innerStream, leaveInnerStreamOpen: true, AllowAnyCertificate);

        private bool ValidateCertificate(
            object sender,
            X509Certificate certificate,
            X509Chain chain,
            SslPolicyErrors sslPolicyErrors)
        {
            List<X509Certificate2> trustedCertificates;

            lock (_lockObj)
            {
                trustedCertificates = _trustedCertificates;
            }

            chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
            chain.ChainPolicy.RevocationFlag = X509RevocationFlag.ExcludeRoot;
            chain.ChainPolicy.VerificationFlags = X509VerificationFlags.AllowUnknownCertificateAuthority;

            foreach (var cert in trustedCertificates)
            {
                chain.ChainPolicy.ExtraStore.Add(cert);
            }

            var certificate2 = new X509Certificate2(certificate);
            var exceptions = new List<Exception>();
            bool success = false;

            foreach (var trustedCertificate in trustedCertificates)
            {
                try
                {
                    ValidateCertificate(certificate2, trustedCertificate, chain);
                    success = true;
                }
                catch (Exception ex)
                {
                    exceptions.Add(ex);
                }
            }

            if (!success)
                throw new AggregateException(exceptions);

            return success;
        }

        private static bool AllowAnyCertificate(object sender,
            X509Certificate certificate, X509Chain chain, SslPolicyErrors sslPolicyErrors) => true;

        // https://stackoverflow.com/questions/6497040/how-do-i-validate-that-a-certificate-was-created-by-a-particular-certification-a
        private static void ValidateCertificate(
            X509Certificate2 certificateToValidate,
            X509Certificate2 authority,
            X509Chain chain)
        {
            bool isChainValid = chain.Build(certificateToValidate);

            if (!isChainValid)
            {
                string certificateErrorsString;

                if (chain.ChainStatus.Length > 0)
                {
                    var errors = chain.ChainStatus
                        .Select(s => string.Format($"{s.StatusInformation} ({s.Status})"));

                    certificateErrorsString = string.Join(", ", errors);
                }
                else
                {
                    certificateErrorsString = "Unknown errors.";
                }

                throw new Exception("Trust chain did not complete to the known authority anchor." +
                    $"Errors: {certificateErrorsString}");
            }

            // This piece makes sure it actually matches your known root
            var valid = chain.ChainElements
                .Cast<X509ChainElement>()
                .Any(x => x.Certificate.Thumbprint == authority.Thumbprint);

            if (!valid)
            {
                throw new Exception("Trust chain did not complete to the known authority anchor." +
                    "Thumbprints did not match.");
            }
        }
    }
}
