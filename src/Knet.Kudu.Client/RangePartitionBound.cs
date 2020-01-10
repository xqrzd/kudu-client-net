namespace Knet.Kudu.Client
{
    /// <summary>
    /// Specifies whether a range partition bound is inclusive or exclusive.
    /// </summary>
    public enum RangePartitionBound
    {
        /// <summary>
        /// An exclusive range partition bound.
        /// </summary>
        Exclusive,
        /// <summary>
        /// An inclusive range partition bound.
        /// </summary>
        Inclusive
    }
}