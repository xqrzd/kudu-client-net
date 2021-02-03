using System;
using System.Buffers;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Knet.Kudu.Client.Connection;
using Knet.Kudu.Client.Protocol;
using Knet.Kudu.Client.Protocol.Rpc;

namespace Knet.Kudu.Client.Util
{
    public static class Extensions
    {
        public static string ToStringUtf8(this byte[] source) =>
            Encoding.UTF8.GetString(source);

        public static byte[] ToUtf8ByteArray(this string source) =>
            Encoding.UTF8.GetBytes(source);

        public static HostAndPort ToHostAndPort(this HostPortPB hostPort) =>
            new HostAndPort(hostPort.Host, (int)hostPort.Port);

        public static int SequenceCompareTo<T>(this T[] array, ReadOnlySpan<T> other)
            where T : IComparable<T> => MemoryExtensions.SequenceCompareTo(array, other);

        public static bool SequenceEqual<T>(this T[] array, ReadOnlySpan<T> other)
            where T : IEquatable<T> => MemoryExtensions.SequenceEqual(array, other);

        public static bool IsCompletedSuccessfully(this Task task)
        {
#if NETSTANDARD2_0
            return task.IsCompleted && !(task.IsCanceled || task.IsFaulted);
#else
            return task.IsCompletedSuccessfully;
#endif
        }

        public static async Task WithCancellation(
            this Task task, CancellationToken cancellationToken)
        {
            if (cancellationToken.CanBeCanceled)
            {
                var tcs = new TaskCompletionSource<object>(
                    TaskCreationOptions.RunContinuationsAsynchronously);

                using var registration = cancellationToken.Register(
                    s => ((TaskCompletionSource<object>)s).TrySetCanceled(),
                    state: tcs,
                    useSynchronizationContext: false);

                var eitherTask = await Task.WhenAny(task, tcs.Task)
                    .ConfigureAwait(false);
                await eitherTask.ConfigureAwait(false);
            }
            else
            {
                await task.ConfigureAwait(false);
            }
        }

        public static int NextClearBit(this BitArray array, int from)
        {
            var length = array.Count;

            for (int i = from; i < length; i++)
            {
                if (!array.Get(i))
                    return i;
            }

            return Math.Max(from, length);
        }

        public static int NextSetBit(this BitArray array, int from)
        {
            var length = array.Count;

            for (int i = from; i < length; i++)
            {
                if (array.Get(i))
                    return i;
            }

            return -1;
        }

        public static int Cardinality(this BitArray array)
        {
            int count = 0;
            var length = array.Count;

            for (int i = 0; i < length; i++)
            {
                if (array.Get(i))
                    count++;
            }

            return count;
        }

        public static bool GetBit(this byte[] input, int offset, int index)
        {
            return (input[(int)((uint)offset + ((uint)index / 8))] & (1 << (int)((uint)index % 8))) != 0;
        }

        public static bool GetBit(this ReadOnlySpan<byte> input, int offset, int index)
        {
            return (input[(int)((uint)offset + ((uint)index / 8))] & (1 << (int)((uint)index % 8))) != 0;
        }

        public static void SetBit(this byte[] input, int offset, int index)
        {
            input[(int)((uint)offset + ((uint)index / 8))] |= (byte)(1 << (int)((uint)index % 8));
        }

        internal static ArrayBufferWriter<T> Clone<T>(this ArrayBufferWriter<T> writer)
        {
            var newWriter = new ArrayBufferWriter<T>(writer.Capacity);
            newWriter.Write(writer.WrittenSpan);
            return newWriter;
        }

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

        internal static int GetScannerBatchSizeEstimate(this KuduSchema schema)
        {
            if (schema.VarLengthColumnCount == 0)
            {
                // No variable length data, we can do an
                // exact ideal estimate here.
                return 1024 * 1024 - schema.RowSize;
            }
            else
            {
                // Assume everything evens out.
                // Most of the time it probably does.
                return 1024 * 1024;
            }
        }
    }
}
