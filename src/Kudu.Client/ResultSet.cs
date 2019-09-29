using System;
using System.Runtime.CompilerServices;

namespace Kudu.Client
{
    public class ResultSet
    {
        private readonly Schema _schema;
        private readonly ReadOnlyMemory<byte> _rowAlloc;
        private readonly ReadOnlyMemory<byte> _indirectData;
        private readonly int[] _columnOffsets;
        private readonly int _rowSize;
        private readonly bool _hasNullableColumns;

        public int Count { get; }

        public ResultSet(
            Schema schema,
            int numRows,
            ReadOnlyMemory<byte> rowAlloc,
            ReadOnlyMemory<byte> indirectData)
        {
            _schema = schema;
            _rowAlloc = rowAlloc;
            _indirectData = indirectData;
            Count = numRows;

            int columnOffsetsSize = schema.Columns.Count;
            if (schema.HasNullableColumns)
            {
                columnOffsetsSize++;
                _hasNullableColumns = true;
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
        }

        public RowResult this[int index]
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => new RowResult(
                this,
                _rowAlloc.Span.Slice(index * _rowSize, _rowSize),
                _indirectData.Span);
        }

        internal int GetOffset(int columnIndex)
        {
            return _columnOffsets[columnIndex];
        }

        internal int GetNullBitSetOffset()
        {
            return _columnOffsets[_columnOffsets.Length - 1];
        }

        internal int GetColumnIndex(string columnName)
        {
            return _schema.GetColumnIndex(columnName);
        }

        public Enumerator GetEnumerator() => new Enumerator(this);

        public ref struct Enumerator
        {
            private readonly ResultSet _resultSet;
            private int _index;

            internal Enumerator(ResultSet resultSet)
            {
                _resultSet = resultSet;
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
                get => _resultSet[_index];
            }
        }
    }
}
