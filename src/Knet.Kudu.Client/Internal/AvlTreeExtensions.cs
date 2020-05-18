using System;
using Knet.Kudu.Client.Tablet;

namespace Knet.Kudu.Client.Internal
{
    public static class AvlTreeExtensions
    {
        public static RemoteTablet GetFloor(
            this AvlTree avlTree, ReadOnlySpan<byte> partitionKey)
        {
            avlTree.SearchLeftRight(
                partitionKey,
                out RemoteTablet left,
                out _);

            return left;
        }

        public static TabletLocationEntry FloorEntry(
            this AvlTree2 avlTree, ReadOnlySpan<byte> partitionKey)
        {
            avlTree.SearchLeftRight(
                    partitionKey,
                    out TabletLocationEntry left,
                    out _);

            return left;
        }
    }
}
