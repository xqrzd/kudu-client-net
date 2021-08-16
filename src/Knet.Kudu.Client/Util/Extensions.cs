using System;
using System.Buffers;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Knet.Kudu.Client.Connection;
using Knet.Kudu.Client.Internal;
using Knet.Kudu.Client.Protobuf;

namespace Knet.Kudu.Client.Util
{
    public static class Extensions
    {
        public static string ToStringUtf8(this byte[] source) =>
            Encoding.UTF8.GetString(source);

        public static byte[] ToUtf8ByteArray(this string source) =>
            Encoding.UTF8.GetBytes(source);

        public static HostAndPort ToHostAndPort(this HostPortPB hostPort) =>
            new(hostPort.Host, (int)hostPort.Port);

        public static int SequenceCompareTo<T>(this T[] array, ReadOnlySpan<T> other)
            where T : IComparable<T> => MemoryExtensions.SequenceCompareTo(array, other);

        public static bool SequenceEqual<T>(this T[] array, ReadOnlySpan<T> other)
            where T : IEquatable<T> => MemoryExtensions.SequenceEqual(array, other);

        // https://github.com/dotnet/runtime/blob/master/src/libraries/Common/src/System/Threading/Tasks/TaskTimeoutExtensions.cs
        public static Task WithCancellation(this Task task, CancellationToken cancellationToken)
        {
            if (task.IsCompleted || !cancellationToken.CanBeCanceled)
            {
                return task;
            }

            if (cancellationToken.IsCancellationRequested)
            {
                return Task.FromCanceled(cancellationToken);
            }

            return WithCancellationCore(task, cancellationToken);

            static async Task WithCancellationCore(Task task, CancellationToken cancellationToken)
            {
                var tcs = new TaskCompletionSource();
#if NET5_0_OR_GREATER
                using var _ = cancellationToken.UnsafeRegister(
                    static s => ((TaskCompletionSource)s).TrySetResult(), tcs);
#else
                using var _ = cancellationToken.Register(
                    static s => ((TaskCompletionSource)s).TrySetResult(), tcs);
#endif
                if (task != await Task.WhenAny(task, tcs.Task).ConfigureAwait(false))
                {
                    throw new TaskCanceledException(Task.FromCanceled(cancellationToken));
                }

                task.GetAwaiter().GetResult(); // Already completed; propagate any exception.
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
    }
}
