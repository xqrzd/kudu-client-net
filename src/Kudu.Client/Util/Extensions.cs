using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Kudu.Client.Connection;
using Kudu.Client.Protocol;
using Kudu.Client.Protocol.Rpc;

namespace Kudu.Client.Util
{
    public static class Extensions
    {
        public static string ToStringUtf8(this byte[] source) =>
            Encoding.UTF8.GetString(source);

        public static byte[] ToUtf8ByteArray(this string source) =>
            Encoding.UTF8.GetBytes(source);

        public static HostAndPort ToHostAndPort(this HostPortPB hostPort) =>
            new HostAndPort(hostPort.Host, (int)hostPort.Port);

        public static void SwapMostSignificantBitBigEndian(this byte[] span) =>
            span[0] ^= 1 << 7;

        public static void SwapMostSignificantBitBigEndian(this Span<byte> span) =>
            span[0] ^= 1 << 7;

        /// <summary>
        /// Obtains the data as a list; if it is *already* a list, the original object is returned without
        /// any duplication; otherwise, ToList() is invoked.
        /// </summary>
        /// <typeparam name="T">The type of element in the list.</typeparam>
        /// <param name="source">The enumerable to return as a list.</param>
        public static List<T> AsList<T>(this IEnumerable<T> source) =>
            (source == null || source is List<T>) ? (List<T>)source : source.ToList();

        public static int GetContentHashCode(this byte[] source)
        {
            if (source == null)
                return 0;

            int result = 1;
            foreach (byte element in source)
                result = 31 * result + element;

            return result;
        }

        public static bool HasRpcFeature(this NegotiatePB negotiatePb, RpcFeatureFlag flag)
        {
            return negotiatePb.SupportedFeatures != null &&
                negotiatePb.SupportedFeatures.Contains(flag);
        }
    }
}
