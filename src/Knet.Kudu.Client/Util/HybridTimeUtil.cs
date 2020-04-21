using System;

namespace Knet.Kudu.Client.Util
{
    /// <summary>
    /// Set of common utility methods to handle HybridTime and related timestamps.
    /// </summary>
    public class HybridTimeUtil
    {
        public const int HybridTimeNumBitsToShift = 12;
        public const int HybridTimeLogicalBitsMask = (1 << HybridTimeNumBitsToShift) - 1;

        /// <summary>
        /// Converts the provided timestamp in microseconds to the HybridTime
        /// timestamp format. Logical bits are set to 0.
        /// </summary>
        /// <param name="timestampInMicros">
        /// The value of the timestamp, must be greater than 0.
        /// </param>
        public static long ClockTimestampToHtTimestamp(long timestampInMicros)
        {
            if (timestampInMicros < 0)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(timestampInMicros), "Timestamp cannot be less than 0");
            }

            return timestampInMicros << HybridTimeNumBitsToShift;
        }

        /// <summary>
        /// Extracts the physical and logical values from an HT timestamp.
        /// </summary>
        /// <param name="htTimestamp">The encoded HT timestamp.</param>
        public static (long timestampMicros, long logicalValue) HtTimestampToPhysicalAndLogical(
            long htTimestamp)
        {
            long timestampInMicros = htTimestamp >> HybridTimeNumBitsToShift;
            long logicalValues = htTimestamp & HybridTimeLogicalBitsMask;
            return (timestampInMicros, logicalValues);
        }

        /// <summary>
        /// Encodes separate physical and logical components into a single HT timestamp.
        /// </summary>
        /// <param name="physical">The physical component, in microseconds.</param>
        /// <param name="logical">The logical component.</param>
        public static long PhysicalAndLogicalToHtTimestamp(long physical, long logical)
        {
            return (physical << HybridTimeNumBitsToShift) + logical;
        }
    }
}
