using System;
using System.Collections.Generic;
using System.IO.Pipelines;
using Knet.Kudu.Client.Connection;
using Knet.Kudu.Client.Util;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Knet.Kudu.Client
{
    public class KuduClientBuilder
    {
        private readonly IReadOnlyList<HostAndPort> _masterAddresses;
        private ILoggerFactory _loggerFactory = NullLoggerFactory.Instance;
        private TimeSpan _defaultAdminOperationTimeout = TimeSpan.FromSeconds(30);
        private TimeSpan _defaultOperationTimeout = TimeSpan.FromSeconds(30);
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
        }

        public KuduClientBuilder(IReadOnlyList<HostAndPort> masterAddresses)
        {
            _masterAddresses = masterAddresses;
        }

        public KuduClientBuilder SetLoggerFactory(ILoggerFactory loggerFactory)
        {
            _loggerFactory = loggerFactory;
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
                _defaultAdminOperationTimeout,
                _defaultOperationTimeout,
                _sendPipeOptions,
                _receivePipeOptions);

            return options;
        }

        public KuduClient Build()
        {
            var options = BuildOptions();
            return new KuduClient(options, _loggerFactory);
        }
    }
}
