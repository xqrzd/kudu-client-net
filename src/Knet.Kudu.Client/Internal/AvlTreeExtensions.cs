using System;
using Knet.Kudu.Client.Tablet;

namespace Knet.Kudu.Client.Internal
{
    public static class AvlTreeExtensions
    {
        public static TabletLocationEntry FloorEntry(
            this AvlTree avlTree, ReadOnlySpan<byte> partitionKey)
        {
            avlTree.SearchLeftRight(
                partitionKey,
                out TabletLocationEntry left,
                out _);

            return left;
        }
    }
}
