using System;
using System.Collections.Generic;
using System.IO.Pipelines;
using Kudu.Client.Connection;
using Kudu.Client.Util;

namespace Kudu.Client
{
    public class KuduClientBuilder
    {
        private readonly IReadOnlyList<HostAndPort> _masterAddresses;
        private string _kerberosSpn;
        private string _tlsHost;
        private TimeSpan _defaultAdminOperationTimeout;
        private TimeSpan _defaultOperationTimeout;
        private PipeOptions _sendPipeOptions;
        private PipeOptions _receivePipeOptions;

        public KuduClientBuilder(string masterAddresses)
        {
            var masters = masterAddresses.Split(',');
            var results = new List<HostAndPort>(masters.Length);

            foreach (var master in masters)
            {
                var hostPort = EndpointParser.TryParse(master.Trim(), 7051);
                results.Add(hostPort);
            }

            _masterAddresses = results;
            _defaultAdminOperationTimeout = TimeSpan.FromSeconds(30);
            _defaultOperationTimeout = TimeSpan.FromSeconds(30);
        }

        public KuduClientBuilder(IReadOnlyList<HostAndPort> masterAddresses)
        {
            _masterAddresses = masterAddresses;
        }

        public KuduClientBuilder SetKerberosSpn(string kerberosSpn)
        {
            _kerberosSpn = kerberosSpn;
            return this;
        }

        public KuduClientBuilder SetTlsHost(string tlsHost)
        {
            _tlsHost = tlsHost;
            return this;
        }

        public KuduClientBuilder SetDefaultAdminOperationTimeout(TimeSpan timeout)
        {
            _defaultAdminOperationTimeout = timeout;
            return this;
        }

        public KuduClientBuilder SetDefaultOperationTimeout(TimeSpan timeout)
        {
            _defaultOperationTimeout = timeout;
            return this;
        }

        public KuduClientBuilder SetSendPipeOptions(PipeOptions options)
        {
            _sendPipeOptions = options;
            return this;
        }

        public KuduClientBuilder SetReceivePipeOptions(PipeOptions options)
        {
            _receivePipeOptions = options;
            return this;
        }

        public KuduClientOptions BuildOptions()
        {
            var options = new KuduClientOptions(
                _masterAddresses,
                _kerberosSpn,
                _tlsHost,
                _defaultAdminOperationTimeout,
                _defaultOperationTimeout,
                _sendPipeOptions,
                _receivePipeOptions);

            return options;
        }

        public KuduClient Build()
        {
            var options = BuildOptions();
            return new KuduClient(options);
        }
    }
}
