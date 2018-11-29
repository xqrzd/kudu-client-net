//https://github.com/bitlush/avl-tree-c-sharp
//MIT License

//Copyright(c) 2012 Keith Wood

//Permission is hereby granted, free of charge, to any person obtaining a copy
//of this software and associated documentation files (the "Software"), to deal
//in the Software without restriction, including without limitation the rights
//to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
//copies of the Software, and to permit persons to whom the Software is
//furnished to do so, subject to the following conditions:

//The above copyright notice and this permission notice shall be included in all
//copies or substantial portions of the Software.

//THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
//IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
//FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
//AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
//LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
//OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
//SOFTWARE.

using System;
using System.Collections;
using System.Collections.Generic;
using Kudu.Client.Tablet;

namespace Kudu.Client.Internal
{
    public class AvlTree : IEnumerable<AvlNode>
    {
        private AvlNode _root;

        public AvlNode Root
        {
            get
            {
                return _root;
            }
        }

        public IEnumerator<AvlNode> GetEnumerator()
        {
            throw new NotImplementedException();
            //return new AvlNodeEnumerator<TKey, TValue>(_root);
        }

        public bool Search(ReadOnlySpan<byte> partitionKey, out RemoteTablet value)
        {
            AvlNode node = _root;

            while (node != null)
            {
                int compare = partitionKey.SequenceCompareTo(node.Tablet.Partition.PartitionKeyStart);

                if (compare < 0)
                {
                    node = node.Left;
                }
                else if (compare > 0)
                {
                    node = node.Right;
                }
                else
                {
                    value = node.Tablet;

                    return true;
                }
            }

            value = default;

            return false;
        }

        public bool Insert(RemoteTablet tablet)
        {
            AvlNode node = _root;

            while (node != null)
            {
                int compare = tablet.Partition.CompareTo(node.Tablet.Partition);

                if (compare < 0)
                {
                    AvlNode left = node.Left;

                    if (left == null)
                    {
                        node.Left = new AvlNode { Tablet = tablet, Parent = node };

                        InsertBalance(node, 1);

                        return true;
                    }
                    else
                    {
                        node = left;
                    }
                }
                else if (compare > 0)
                {
                    AvlNode right = node.Right;

                    if (right == null)
                    {
                        node.Right = new AvlNode { Tablet = tablet, Parent = node };

                        InsertBalance(node, -1);

                        return true;
                    }
                    else
                    {
                        node = right;
                    }
                }
                else
                {
                    //node.Value = value;
                    node.Tablet = tablet;

                    return false;
                }
            }

            _root = new AvlNode { Tablet = tablet };

            return true;
        }

        private void InsertBalance(AvlNode node, int balance)
        {
            while (node != null)
            {
                balance = (node.Balance += balance);

                if (balance == 0)
                {
                    return;
                }
                else if (balance == 2)
                {
                    if (node.Left.Balance == 1)
                    {
                        RotateRight(node);
                    }
                    else
                    {
                        RotateLeftRight(node);
                    }

                    return;
                }
                else if (balance == -2)
                {
                    if (node.Right.Balance == -1)
                    {
                        RotateLeft(node);
                    }
                    else
                    {
                        RotateRightLeft(node);
                    }

                    return;
                }

                AvlNode parent = node.Parent;

                if (parent != null)
                {
                    balance = parent.Left == node ? 1 : -1;
                }

                node = parent;
            }
        }

        private AvlNode RotateLeft(AvlNode node)
        {
            AvlNode right = node.Right;
            AvlNode rightLeft = right.Left;
            AvlNode parent = node.Parent;

            right.Parent = parent;
            right.Left = node;
            node.Right = rightLeft;
            node.Parent = right;

            if (rightLeft != null)
            {
                rightLeft.Parent = node;
            }

            if (node == _root)
            {
                _root = right;
            }
            else if (parent.Right == node)
            {
                parent.Right = right;
            }
            else
            {
                parent.Left = right;
            }

            right.Balance++;
            node.Balance = -right.Balance;

            return right;
        }

        private AvlNode RotateRight(AvlNode node)
        {
            AvlNode left = node.Left;
            AvlNode leftRight = left.Right;
            AvlNode parent = node.Parent;

            left.Parent = parent;
            left.Right = node;
            node.Left = leftRight;
            node.Parent = left;

            if (leftRight != null)
            {
                leftRight.Parent = node;
            }

            if (node == _root)
            {
                _root = left;
            }
            else if (parent.Left == node)
            {
                parent.Left = left;
            }
            else
            {
                parent.Right = left;
            }

            left.Balance--;
            node.Balance = -left.Balance;

            return left;
        }

        private AvlNode RotateLeftRight(AvlNode node)
        {
            AvlNode left = node.Left;
            AvlNode leftRight = left.Right;
            AvlNode parent = node.Parent;
            AvlNode leftRightRight = leftRight.Right;
            AvlNode leftRightLeft = leftRight.Left;

            leftRight.Parent = parent;
            node.Left = leftRightRight;
            left.Right = leftRightLeft;
            leftRight.Left = left;
            leftRight.Right = node;
            left.Parent = leftRight;
            node.Parent = leftRight;

            if (leftRightRight != null)
            {
                leftRightRight.Parent = node;
            }

            if (leftRightLeft != null)
            {
                leftRightLeft.Parent = left;
            }

            if (node == _root)
            {
                _root = leftRight;
            }
            else if (parent.Left == node)
            {
                parent.Left = leftRight;
            }
            else
            {
                parent.Right = leftRight;
            }

            if (leftRight.Balance == -1)
            {
                node.Balance = 0;
                left.Balance = 1;
            }
            else if (leftRight.Balance == 0)
            {
                node.Balance = 0;
                left.Balance = 0;
            }
            else
            {
                node.Balance = -1;
                left.Balance = 0;
            }

            leftRight.Balance = 0;

            return leftRight;
        }

        private AvlNode RotateRightLeft(AvlNode node)
        {
            AvlNode right = node.Right;
            AvlNode rightLeft = right.Left;
            AvlNode parent = node.Parent;
            AvlNode rightLeftLeft = rightLeft.Left;
            AvlNode rightLeftRight = rightLeft.Right;

            rightLeft.Parent = parent;
            node.Right = rightLeftLeft;
            right.Left = rightLeftRight;
            rightLeft.Right = right;
            rightLeft.Left = node;
            right.Parent = rightLeft;
            node.Parent = rightLeft;

            if (rightLeftLeft != null)
            {
                rightLeftLeft.Parent = node;
            }

            if (rightLeftRight != null)
            {
                rightLeftRight.Parent = right;
            }

            if (node == _root)
            {
                _root = rightLeft;
            }
            else if (parent.Right == node)
            {
                parent.Right = rightLeft;
            }
            else
            {
                parent.Left = rightLeft;
            }

            if (rightLeft.Balance == 1)
            {
                node.Balance = 0;
                right.Balance = -1;
            }
            else if (rightLeft.Balance == 0)
            {
                node.Balance = 0;
                right.Balance = 0;
            }
            else
            {
                node.Balance = 1;
                right.Balance = 0;
            }

            rightLeft.Balance = 0;

            return rightLeft;
        }

        public bool Delete(ReadOnlySpan<byte> partitionKey)
        {
            AvlNode node = _root;

            while (node != null)
            {
                int compare = partitionKey.SequenceCompareTo(node.Tablet.Partition.PartitionKeyStart);

                if (compare < 0)
                {
                    node = node.Left;
                }
                else if (compare > 0)
                {
                    node = node.Right;
                }
                else
                {
                    AvlNode left = node.Left;
                    AvlNode right = node.Right;

                    if (left == null)
                    {
                        if (right == null)
                        {
                            if (node == _root)
                            {
                                _root = null;
                            }
                            else
                            {
                                AvlNode parent = node.Parent;

                                if (parent.Left == node)
                                {
                                    parent.Left = null;

                                    DeleteBalance(parent, -1);
                                }
                                else
                                {
                                    parent.Right = null;

                                    DeleteBalance(parent, 1);
                                }
                            }
                        }
                        else
                        {
                            Replace(node, right);

                            DeleteBalance(node, 0);
                        }
                    }
                    else if (right == null)
                    {
                        Replace(node, left);

                        DeleteBalance(node, 0);
                    }
                    else
                    {
                        AvlNode successor = right;

                        if (successor.Left == null)
                        {
                            AvlNode parent = node.Parent;

                            successor.Parent = parent;
                            successor.Left = left;
                            successor.Balance = node.Balance;
                            left.Parent = successor;

                            if (node == _root)
                            {
                                _root = successor;
                            }
                            else
                            {
                                if (parent.Left == node)
                                {
                                    parent.Left = successor;
                                }
                                else
                                {
                                    parent.Right = successor;
                                }
                            }

                            DeleteBalance(successor, 1);
                        }
                        else
                        {
                            while (successor.Left != null)
                            {
                                successor = successor.Left;
                            }

                            AvlNode parent = node.Parent;
                            AvlNode successorParent = successor.Parent;
                            AvlNode successorRight = successor.Right;

                            if (successorParent.Left == successor)
                            {
                                successorParent.Left = successorRight;
                            }
                            else
                            {
                                successorParent.Right = successorRight;
                            }

                            if (successorRight != null)
                            {
                                successorRight.Parent = successorParent;
                            }

                            successor.Parent = parent;
                            successor.Left = left;
                            successor.Balance = node.Balance;
                            successor.Right = right;
                            right.Parent = successor;
                            left.Parent = successor;

                            if (node == _root)
                            {
                                _root = successor;
                            }
                            else
                            {
                                if (parent.Left == node)
                                {
                                    parent.Left = successor;
                                }
                                else
                                {
                                    parent.Right = successor;
                                }
                            }

                            DeleteBalance(successorParent, -1);
                        }
                    }

                    return true;
                }
            }

            return false;
        }

        private void DeleteBalance(AvlNode node, int balance)
        {
            while (node != null)
            {
                balance = (node.Balance += balance);

                if (balance == 2)
                {
                    if (node.Left.Balance >= 0)
                    {
                        node = RotateRight(node);

                        if (node.Balance == -1)
                        {
                            return;
                        }
                    }
                    else
                    {
                        node = RotateLeftRight(node);
                    }
                }
                else if (balance == -2)
                {
                    if (node.Right.Balance <= 0)
                    {
                        node = RotateLeft(node);

                        if (node.Balance == 1)
                        {
                            return;
                        }
                    }
                    else
                    {
                        node = RotateRightLeft(node);
                    }
                }
                else if (balance != 0)
                {
                    return;
                }

                AvlNode parent = node.Parent;

                if (parent != null)
                {
                    balance = parent.Left == node ? -1 : 1;
                }

                node = parent;
            }
        }

        public void ClearRange(ReadOnlySpan<byte> from, ReadOnlySpan<byte> to)
        {
            var results = new List<byte[]>();
            GetInRange(_root, from, to, results);

            foreach (var result in results)
            {
                Delete(result);
            }
        }

        private void GetInRange(AvlNode node,
            ReadOnlySpan<byte> min, ReadOnlySpan<byte> max, List<byte[]> results)
        {
            if (node == null)
                return;

            int compare1 = min.SequenceCompareTo(node.Tablet.Partition.PartitionKeyStart);
            int compare2 = max.SequenceCompareTo(node.Tablet.Partition.PartitionKeyStart);

            /* Since the desired o/p is sorted, recurse for left subtree first 
             If root->data is greater than k1, then only we can get o/p keys 
             in left subtree */
            if (compare1 < 0)
            {
                GetInRange(node.Left, min, max, results);
            }

            /* if root's data lies in range, then prints root's data */
            if (compare1 <= 0 && compare2 > 0)
            {
                //Console.WriteLine(node.Key);
                results.Add(node.Tablet.Partition.PartitionKeyStart);
            }

            /* If root->data is smaller than k2, then only we can get o/p keys 
             in right subtree */
            if (compare2 > 0)
            {
                GetInRange(node.Right, min, max, results);
            }
        }

        //private AvlNode FindRange(ReadOnlySpan<byte> from, ReadOnlySpan<byte> to)
        //{
        //    AvlNode node = _root;

        //    while (node != null)
        //    {
        //        if (from.SequenceCompareTo(node.Tablet.Partition.PartitionKeyStart) > 0)
        //        {
        //            node = node.Right;
        //        }
        //        else
        //        {
        //            if (to.SequenceCompareTo(node.Tablet.Partition.PartitionKeyStart) < 0)
        //            {
        //                node = node.Left;
        //            }
        //            else
        //            {
        //                return node;
        //            }
        //        }
        //    }

        //    return null;
        //}

        public void Clear()
        {
            _root = null;
        }

        public RemoteTablet GetFloorEntry(ReadOnlySpan<byte> partitionKey)
        {
            AvlNode node = GetFloorEntryInternal(partitionKey);
            return node == null ? default : node.Tablet;
        }

        private AvlNode GetFloorEntryInternal(ReadOnlySpan<byte> partitionKey)
        {
            AvlNode p = _root;

            while (p != null)
            {
                int cmp = partitionKey.SequenceCompareTo(p.Tablet.Partition.PartitionKeyStart);
                if (cmp > 0)
                {
                    if (p.Right != null)
                        p = p.Right;
                    else
                        return p;
                }
                else if (cmp < 0)
                {
                    if (p.Left != null)
                    {
                        p = p.Left;
                    }
                    else
                    {
                        AvlNode parent = p.Parent;
                        AvlNode ch = p;
                        while (parent != null && ch == parent.Left)
                        {
                            ch = parent;
                            parent = parent.Parent;
                        }
                        return parent;
                    }
                }
                else
                    return p;

            }
            return null;
        }

        private static void Replace(AvlNode target, AvlNode source)
        {
            AvlNode left = source.Left;
            AvlNode right = source.Right;

            target.Balance = source.Balance;
            //target.Key = source.Key;
            //target.Value = source.Value;
            target.Tablet = source.Tablet;
            target.Left = left;
            target.Right = right;

            if (left != null)
            {
                left.Parent = target;
            }

            if (right != null)
            {
                right.Parent = target;
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}
