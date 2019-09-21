namespace Kudu.Client.Util
{
    /// <summary>
    /// Set of common utility methods to handle HybridTime and related timestamps.
    /// </summary>
    public class HybridTimeUtil
    {
        public const int HybridTimeNumBitsToShift = 12;
        public const int HybridTimeLogicalBitsMask = (1 << HybridTimeNumBitsToShift) - 1;

        /// <summary>
        /// Encodes separate physical and logical components into a single HT timestamp.
        /// </summary>
        /// <param name="physical">The physical component, in microseconds.</param>
        /// <param name="logical">The logical component.</param>
        public static long PhysicalAndLogicalToHTTimestamp(long physical, long logical)
        {
            return (physical << HybridTimeNumBitsToShift) + logical;
        }
    }
}
