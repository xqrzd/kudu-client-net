using System;

namespace Knet.Kudu.Client.Exceptions
{
    /// <summary>
    /// Exception indicating that an operation attempted to access a
    /// non-covered range partition.
    /// </summary>
    public class NonCoveredRangeException : NonRecoverableException
    {
        public byte[] NonCoveredRangeStart;

        public byte[] NonCoveredRangeEnd;

        public NonCoveredRangeException(
            byte[] nonCoveredRangeStart,
            byte[] nonCoveredRangeEnd)
            : base(KuduStatus.NotFound(GetMessage(
                nonCoveredRangeStart, nonCoveredRangeEnd)))
        {
            NonCoveredRangeStart = nonCoveredRangeStart;
            NonCoveredRangeEnd = nonCoveredRangeEnd;
        }

        private static string GetMessage(byte[] start, byte[] end)
        {
            var startStr = start.Length == 0 ? "<start>" : BitConverter.ToString(start);
            var endStr = end.Length == 0 ? "<end>" : BitConverter.ToString(end);

            return $"[{startStr}, {endStr})";
        }
    }
}
