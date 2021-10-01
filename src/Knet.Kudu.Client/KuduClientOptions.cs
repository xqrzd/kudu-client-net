using System;
using System.Collections.Generic;
using System.IO.Pipelines;
using Knet.Kudu.Client.Connection;

namespace Knet.Kudu.Client;

public class KuduClientOptions
{
    public IReadOnlyList<HostAndPort> MasterAddresses { get; }

    public TimeSpan DefaultOperationTimeout { get; }

    public string? SaslProtocolName { get; }

    public PipeOptions SendPipeOptions { get; }

    public PipeOptions ReceivePipeOptions { get; }

    public KuduClientOptions(
        IReadOnlyList<HostAndPort> masterAddresses,
        TimeSpan defaultOperationTimeout,
        string? saslProtocolName,
        PipeOptions sendPipeOptions,
        PipeOptions receivePipeOptions)
    {
        MasterAddresses = masterAddresses;
        DefaultOperationTimeout = defaultOperationTimeout;
        SaslProtocolName = saslProtocolName;
        SendPipeOptions = sendPipeOptions;
        ReceivePipeOptions = receivePipeOptions;
    }
}
