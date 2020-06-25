namespace Knet.Kudu.Client
{
    public class WriteResponse
    {
        /// <summary>
        /// The HybridTime-encoded write timestamp.
        /// </summary>
        public long WriteTimestampRaw { get; }

        public WriteResponse(long writeTimestampRaw)
        {
            WriteTimestampRaw = writeTimestampRaw;
        }
    }
}
