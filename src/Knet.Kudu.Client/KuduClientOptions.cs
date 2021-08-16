using System;
using System.Collections.Generic;
using System.IO.Pipelines;
using Knet.Kudu.Client.Connection;

namespace Knet.Kudu.Client
{
    public class KuduClientOptions
    {
        private static readonly PipeOptions _defaultSendOptions = new(
            readerScheduler: PipeScheduler.ThreadPool,
            writerScheduler: PipeScheduler.ThreadPool,
            pauseWriterThreshold: 1024 * 1024 * 4,  // 4MB
            resumeWriterThreshold: 1024 * 1024 * 2, // 2MB
            minimumSegmentSize: 4096,
            useSynchronizationContext: false);

        private static readonly PipeOptions _defaultReceiveOptions = new(
            readerScheduler: PipeScheduler.ThreadPool,
            writerScheduler: PipeScheduler.ThreadPool,
            pauseWriterThreshold: 1024 * 1024 * 128, // 128MB
            resumeWriterThreshold: 1024 * 1024 * 64, // 64MB
            minimumSegmentSize: 1024 * 1024, // 1MB
            useSynchronizationContext: false);

        public IReadOnlyList<HostAndPort> MasterAddresses { get; }

        public TimeSpan DefaultOperationTimeout { get; }

        public string SaslProtocolName { get; }

        public PipeOptions SendPipeOptions { get; }

        public PipeOptions ReceivePipeOptions { get; }

        public KuduClientOptions(
            IReadOnlyList<HostAndPort> masterAddresses,
            TimeSpan defaultOperationTimeout,
            string saslProtocolName,
            PipeOptions sendPipeOptions,
            PipeOptions receivePipeOptions)
        {
            MasterAddresses = masterAddresses;
            DefaultOperationTimeout = defaultOperationTimeout;
            SaslProtocolName = saslProtocolName;

            SendPipeOptions = sendPipeOptions ?? _defaultSendOptions;
            ReceivePipeOptions = receivePipeOptions ?? _defaultReceiveOptions;
        }
    }
}
