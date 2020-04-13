using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Linq;

namespace Knet.Kudu.Client.FunctionalTests.Util
{
    public static class TestExtensions
    {
        /// <summary>
        /// Returns a view of the portion of this set whose elements are greater than
        /// (or equal to, if inclusive is true) start.
        /// </summary>
        public static SortedSet<T> TailSet<T>(this SortedSet<T> set, T start, bool inclusive = true)
        {
            if (inclusive)
            {
                return set.GetViewBetween(start, set.Max);
            }
            else
            {
                // There's no overload of GetViewBetween that supports this.
                var comparer = set.Comparer;
                return new SortedSet<T>(set.Where(v => comparer.Compare(v, start) > 0), comparer);
            }
        }

        /// <summary>
        /// Returns a view of the portion of this set whose elements are less than
        /// (or equal to, if inclusive is true) end.
        /// </summary>
        public static SortedSet<T> HeadSet<T>(this SortedSet<T> set, T end, bool inclusive = false)
        {
            if (inclusive)
            {
                return set.GetViewBetween(set.Min, end);
            }
            else
            {
                // There's no overload of GetViewBetween that supports this.
                var comparer = set.Comparer;
                return new SortedSet<T>(set.Where(v => comparer.Compare(v, end) < 0), comparer);
            }
        }

        public static long NextLong(this Random random)
        {
            Span<byte> buffer = stackalloc byte[8];
            random.NextBytes(buffer);
            return BinaryPrimitives.ReadInt64LittleEndian(buffer);
        }
    }
}
