using System;
using System.Collections.Generic;
using System.IO.Pipelines;
using Knet.Kudu.Client.Connection;
using Knet.Kudu.Client.Internal;
using Knet.Kudu.Client.Util;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Knet.Kudu.Client;

public class KuduClientBuilder
{
    private readonly IReadOnlyList<HostAndPort> _masterAddresses;
    private ILoggerFactory _loggerFactory = NullLoggerFactory.Instance;
    private TimeSpan _defaultOperationTimeout = TimeSpan.FromSeconds(30);
    private string _saslProtocolName = "kudu";
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

    public KuduClientBuilder SetDefaultOperationTimeout(TimeSpan timeout)
    {
        _defaultOperationTimeout = timeout;
        return this;
    }

    /// <summary>
    /// <para>
    /// Set the SASL protocol name. SASL protocol name is used when connecting
    /// to a secure (Kerberos-enabled) cluster. It must match the servers'
    /// service principal name (SPN).
    /// </para>
    /// <para>
    /// If not provided, it will use the default SASL protocol name ("kudu").
    /// </para>
    /// </summary>
    public KuduClientBuilder SetSaslProtocolName(string saslProtocolName)
    {
        _saslProtocolName = saslProtocolName;
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
            _defaultOperationTimeout,
            _saslProtocolName,
            _sendPipeOptions,
            _receivePipeOptions);

        return options;
    }

    public KuduClient Build()
    {
        var options = BuildOptions();
        var securityContext = new SecurityContext();
        var systemClock = new SystemClock();
        var connectionFactory = new KuduConnectionFactory(
            options, securityContext, _loggerFactory);

        return new KuduClient(
            options,
            securityContext,
            connectionFactory,
            systemClock,
            _loggerFactory);
    }
}
