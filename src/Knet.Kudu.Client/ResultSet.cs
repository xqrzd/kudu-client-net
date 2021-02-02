using System;
using System.Runtime.CompilerServices;

namespace Knet.Kudu.Client
{
    public class ResultSet
    {
        private readonly ReadOnlyMemory<byte> _rowAlloc;
        private readonly ReadOnlyMemory<byte> _indirectData;
        private readonly int[] _columnOffsets;
        private readonly int _rowSize;

        public KuduSchema Schema { get; }

        public int Count { get; }

        internal bool HasNullableColumns { get; }

        internal int NullBitSetOffset { get; }

        public ResultSet(
            KuduSchema schema,
            int numRows,
            ReadOnlyMemory<byte> rowAlloc,
            ReadOnlyMemory<byte> indirectData)
        {
            Schema = schema;
            _rowAlloc = rowAlloc;
            _indirectData = indirectData;
            Count = numRows;

            int columnOffsetsSize = schema.Columns.Count;
            if (schema.HasNullableColumns)
            {
                columnOffsetsSize++;
                HasNullableColumns = true;
            }

            var columnOffsets = new int[columnOffsetsSize];
            // Empty projection, usually used for quick row counting.
            if (columnOffsetsSize == 0)
            {
                return;
            }

            int currentOffset = 0;
            columnOffsets[0] = currentOffset;
            // Pre-compute the columns offsets in rowData for easier lookups later.
            // If the schema has nullables, we also add the offset for the null bitmap at the end.
            for (int i = 1; i < columnOffsetsSize; i++)
            {
                ColumnSchema column = schema.GetColumn(i - 1);
                int previousSize = column.Size;
                columnOffsets[i] = previousSize + currentOffset;
                currentOffset += previousSize;
            }

            _columnOffsets = columnOffsets;
            _rowSize = schema.RowSize;
            NullBitSetOffset = columnOffsets[columnOffsets.Length - 1];
        }

        internal int GetOffset(int columnIndex)
        {
            return _columnOffsets[columnIndex];
        }

        internal int GetColumnIndex(string columnName)
        {
            return Schema.GetColumnIndex(columnName);
        }

        internal ColumnSchema GetColumnSchema(int columnIndex)
        {
            return Schema.GetColumn(columnIndex);
        }

        public override string ToString() => $"{Count} rows";

        public Enumerator GetEnumerator() => new Enumerator(this);

        public ref struct Enumerator
        {
            private readonly ResultSet _resultSet;
            private readonly ReadOnlySpan<byte> _rowData;
            private readonly ReadOnlySpan<byte> _indirectData;
            private readonly int _rowSize;
            private int _index;

            internal Enumerator(ResultSet resultSet)
            {
                _resultSet = resultSet;
                _rowData = resultSet._rowAlloc.Span;
                _indirectData = _resultSet._indirectData.Span;
                _rowSize = resultSet._rowSize;
                _index = -1;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool MoveNext()
            {
                int index = _index + 1;
                if (index < _resultSet.Count)
                {
                    _index = index;
                    return true;
                }

                return false;
            }

            public RowResult Current
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get
                {
                    var rowData = _rowData.Slice(_index * _rowSize, _rowSize);
                    return new RowResult(_resultSet, rowData, _indirectData);
                }
            }
        }
    }

    public sealed class ResultSetWrapper : ResultSet, IDisposable
    {
        private readonly IDisposable _memory;

        public ResultSetWrapper(
            IDisposable memory,
            KuduSchema schema,
            int numRows,
            ReadOnlyMemory<byte> rowAlloc,
            ReadOnlyMemory<byte> indirectData)
            : base(schema, numRows, rowAlloc, indirectData)
        {
            _memory = memory;
        }

        public void Dispose()
        {
            _memory.Dispose();
        }
    }
}
