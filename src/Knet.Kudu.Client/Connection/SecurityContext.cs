using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using Knet.Kudu.Client.Protocol.Client;
using Knet.Kudu.Client.Protocol.Security;
using ProtoBuf;

namespace Knet.Kudu.Client.Connection
{
    public class SecurityContext : ISecurityContext
    {
        private readonly object _lockObj = new object();

        private SignedTokenPB _authnToken;
        private List<X509Certificate2> _trustedCertificates;
        private bool _isAuthenticationTokenImported;

        /// <summary>
        /// True if the user imported an authentication token to use.
        /// </summary>
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

        /// <summary>
        /// Set the token that we will use to authenticate to servers. Replaces any
        /// prior token.
        /// </summary>
        /// <param name="token">The token to set.</param>
        public void SetAuthenticationToken(SignedTokenPB token)
        {
            lock (_lockObj)
            {
                _authnToken = token;
            }
        }

        /// <summary>
        /// Get the current authentication token, or null if we have no valid token.
        /// </summary>
        public SignedTokenPB GetAuthenticationToken()
        {
            lock (_lockObj)
            {
                return _authnToken;
            }
        }

        /// <summary>
        /// Export serialized authentication data that may be passed to a different
        /// client instance and imported to provide that client the ability to connect
        /// to the cluster.
        /// </summary>
        public ReadOnlyMemory<byte> ExportAuthenticationCredentials()
        {
            var tokenPb = new AuthenticationCredentialsPB();

            lock (_lockObj)
            {
                tokenPb.AuthnToken = _authnToken;

                foreach (var certificate in _trustedCertificates)
                {
                    tokenPb.CaCertDers.Add(certificate.RawData);
                }
            }

            var writer = new ArrayBufferWriter<byte>();

            Serializer.Serialize(writer, tokenPb);

            return writer.WrittenMemory;
        }

        /// <summary>
        /// Import data allowing this client to authenticate to the cluster.
        /// </summary>
        /// <param name="token">The authentication token.</param>
        public void ImportAuthenticationCredentials(ReadOnlyMemory<byte> token)
        {
            var tokenPb = Serializer.Deserialize<AuthenticationCredentialsPB>(token);

            lock (_lockObj)
            {
                if (tokenPb.AuthnToken != null)
                {
                    _authnToken = tokenPb.AuthnToken;
                }

                TrustCertificates(tokenPb.CaCertDers);

                _isAuthenticationTokenImported = true;
            }
        }

        /// <summary>
        /// Mark the given CA cert (provided in DER form) as the trusted CA cert for the
        /// client. Replaces any previously trusted cert.
        /// </summary>
        /// <param name="certDers">The certificates to trust.</param>
        public void TrustCertificates(List<byte[]> certDers)
        {
            var certificates = new List<X509Certificate2>(certDers.Count);

            foreach (var cert in certDers)
            {
                certificates.Add(new X509Certificate2(cert));
            }

            lock (_lockObj)
            {
                _trustedCertificates = certificates;
            }
        }

        /// <summary>
        /// Creates a <see cref="SslStream"/> that trusts the certificates provided
        /// by <see cref="TrustCertificates(List{byte[]})"/>.
        /// </summary>
        /// <param name="innerStream">The stream to wrap.</param>
        public SslStream CreateTlsStream(Stream innerStream) =>
            new SslStream(innerStream, leaveInnerStreamOpen: true, ValidateCertificate);

        /// <summary>
        /// Creates a <see cref="SslStream"/> that trusts all certificates.
        /// </summary>
        /// <param name="innerStream">The stream to wrap.</param>
        public SslStream CreateTlsStreamTrustAll(Stream innerStream) =>
            new SslStream(innerStream, leaveInnerStreamOpen: true, AllowAnyCertificate);

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
