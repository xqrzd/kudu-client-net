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
using Knet.Kudu.Client.Tablet;

namespace Knet.Kudu.Client.Internal
{
    public class AvlTree : IEnumerable<RemoteTablet>
    {
        private AvlNode _root;

        public IEnumerator<RemoteTablet> GetEnumerator()
        {
            return new AvlNodeEnumerator(_root);
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

        public void ClearRange(ReadOnlySpan<byte> from, ReadOnlySpan<byte> to, bool upperBoundActive)
        {
            var results = new List<byte[]>();
            GetInRange(_root, from, to, upperBoundActive, results);

            foreach (var result in results)
            {
                Delete(result);
            }
        }

        private static void GetInRange(AvlNode node,
            ReadOnlySpan<byte> min, ReadOnlySpan<byte> max,
            bool upperBoundActive, List<byte[]> results)
        {
            if (node == null)
                return;

            int compare1 = min.SequenceCompareTo(node.Tablet.Partition.PartitionKeyStart);
            int compare2 = upperBoundActive ? max.SequenceCompareTo(node.Tablet.Partition.PartitionKeyStart) : 1;

            if (compare1 < 0)
            {
                GetInRange(node.Left, min, max, upperBoundActive, results);
            }

            if (compare1 <= 0 && compare2 > 0)
            {
                results.Add(node.Tablet.Partition.PartitionKeyStart);
            }

            if (compare2 > 0)
            {
                GetInRange(node.Right, min, max, upperBoundActive, results);
            }
        }

        public void Clear()
        {
            _root = null;
        }

        public RemoteTablet GetFloor(ReadOnlySpan<byte> partitionKey)
        {
            AvlNode node = GetFloor(_root, partitionKey);
            return node == null ? default : node.Tablet;
        }

        private static AvlNode GetFloor(AvlNode node, ReadOnlySpan<byte> partitionKey)
        {
            while (node != null)
            {
                int compare = partitionKey.SequenceCompareTo(node.Tablet.Partition.PartitionKeyStart);

                if (compare > 0)
                {
                    if (node.Right != null)
                    {
                        node = node.Right;
                    }
                    else
                    {
                        return node;
                    }
                }
                else if (compare < 0)
                {
                    if (node.Left != null)
                    {
                        node = node.Left;
                    }
                    else
                    {
                        AvlNode parent = node.Parent;
                        AvlNode child = node;
                        while (parent != null && child == parent.Left)
                        {
                            child = parent;
                            parent = parent.Parent;
                        }
                        return parent;
                    }
                }
                else
                {
                    return node;
                }
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

        private sealed class AvlNode
        {
            public AvlNode Parent;
            public AvlNode Left;
            public AvlNode Right;
            //public TKey Key;
            //public TValue Value;
            public int Balance;

            public RemoteTablet Tablet;
        }

        private sealed class AvlNodeEnumerator : IEnumerator<RemoteTablet>
        {
            private readonly AvlNode _root;
            private Action _action;
            private AvlNode _current;
            private AvlNode _right;

            public AvlNodeEnumerator(AvlNode root)
            {
                _right = _root = root;
                _action = _root == null ? Action.End : Action.Right;
            }

            public bool MoveNext()
            {
                switch (_action)
                {
                    case Action.Right:
                        _current = _right;

                        while (_current.Left != null)
                        {
                            _current = _current.Left;
                        }

                        _right = _current.Right;
                        _action = _right != null ? Action.Right : Action.Parent;

                        return true;

                    case Action.Parent:
                        while (_current.Parent != null)
                        {
                            AvlNode previous = _current;

                            _current = _current.Parent;

                            if (_current.Left == previous)
                            {
                                _right = _current.Right;
                                _action = _right != null ? Action.Right : Action.Parent;

                                return true;
                            }
                        }

                        _action = Action.End;

                        return false;

                    default:
                        return false;
                }
            }

            public void Reset()
            {
                _right = _root;
                _action = _root == null ? Action.End : Action.Right;
            }

            public RemoteTablet Current
            {
                get
                {
                    return _current.Tablet;
                }
            }

            object IEnumerator.Current
            {
                get
                {
                    return Current;
                }
            }

            public void Dispose()
            {
            }

            enum Action
            {
                Parent,
                Right,
                End
            }
        }
    }
}
