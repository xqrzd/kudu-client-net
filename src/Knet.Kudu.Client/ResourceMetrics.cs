using Knet.Kudu.Client.Protocol.Tserver;

namespace Knet.Kudu.Client
{
    public class ResourceMetrics
    {
        /// <summary>
        /// Number of bytes that were read because of a block cache miss.
        /// </summary>
        public long CfileCacheMissBytes { get; private set; }

        /// <summary>
        /// Number of bytes that were read from the block cache because of a hit.
        /// </summary>
        public long CfileCacheHitBytes { get; private set; }

        /// <summary>
        /// Number of bytes read from disk (or cache) by the scanner.
        /// </summary>
        public long BytesRead { get; private set; }

        /// <summary>
        /// Total time taken between scan rpc requests being accepted and when they
        /// were handled in nanoseconds for this scanner.
        /// </summary>
        public long QueueDurationNanos { get; private set; }

        /// <summary>
        /// Total time taken for all scan rpc requests to complete in nanoseconds
        /// for this scanner.
        /// </summary>
        public long TotalDurationNanos { get; private set; }

        /// <summary>
        /// Total elapsed CPU user time in nanoseconds for all scan rpc requests
        /// for this scanner.
        /// </summary>
        public long CpuUserNanos { get; private set; }

        /// <summary>
        /// Total elapsed CPU system time in nanoseconds for all scan rpc requests
        /// for this scanner.
        /// </summary>
        public long CpuSystemNanos { get; private set; }

        internal void Update(ResourceMetricsPB resourceMetricsPb)
        {
            CfileCacheMissBytes += resourceMetricsPb.CfileCacheMissBytes;
            CfileCacheHitBytes += resourceMetricsPb.CfileCacheHitBytes;
            BytesRead += resourceMetricsPb.BytesRead;
            QueueDurationNanos += resourceMetricsPb.QueueDurationNanos;
            TotalDurationNanos += resourceMetricsPb.TotalDurationNanos;
            CpuUserNanos += resourceMetricsPb.CpuUserNanos;
            CpuSystemNanos += resourceMetricsPb.CpuSystemNanos;
        }
    }
}
