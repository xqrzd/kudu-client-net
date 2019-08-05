using System;
using System.Collections.Generic;
using Kudu.Client.Protocol;

namespace Kudu.Client
{
    /// <summary>
    /// Represents table's schema which is essentially a list of columns.
    /// </summary>
    public class Schema
    {
        /// <summary>
        /// Maps column index to column.
        /// </summary>
        private readonly ColumnSchema[] _columnsByIndex;

        /// <summary>
        /// Maps column name to column index.
        /// </summary>
        private readonly Dictionary<string, int> _columnsByName;

        /// <summary>
        /// Maps columnId to column index.
        /// </summary>
        private readonly Dictionary<int, int> _columnsById;

        /// <summary>
        /// Maps column index to data index.
        /// </summary>
        private readonly int[] _columnOffsets;

        /// <summary>
        /// The size of all fixed-length columns.
        /// </summary>
        public int RowAllocSize { get; }

        public int VarLengthColumnCount { get; }

        public bool HasNullableColumns { get; }

        public Schema(List<ColumnSchema> columns)
            : this(columns, null) { }

        public Schema(List<ColumnSchema> columns, List<int> columnIds)
        {
            var hasColumnIds = columnIds != null;
            if (hasColumnIds)
                _columnsById = new Dictionary<int, int>(columns.Count);

            _columnsByName = new Dictionary<string, int>(columns.Count);
            _columnsById = new Dictionary<int, int>(columns.Count);
            _columnOffsets = new int[columns.Count];

            for (int i = 0; i < columns.Count; i++)
            {
                var column = columns[i];

                if (column.Type == KuduType.String || column.Type == KuduType.Binary)
                {
                    _columnOffsets[i] = VarLengthColumnCount;
                    VarLengthColumnCount++;

                    // Don't increment size here, these types are stored separately
                    // in PartialRow (_varLengthData).
                }
                else
                {
                    _columnOffsets[i] = RowAllocSize;
                    RowAllocSize += column.Size;
                }

                HasNullableColumns |= column.IsNullable;
                _columnsByName.Add(column.Name, i);

                if (hasColumnIds)
                    _columnsById.Add(columnIds[i], i);

                // TODO: store primary key columns
            }

            _columnsByIndex = new ColumnSchema[columns.Count];
            columns.CopyTo(_columnsByIndex);
        }

        public Schema(SchemaPB schema)
        {
            var columns = schema.Columns;

            var size = 0;
            var varLenCnt = 0;
            var hasNulls = false;
            var columnsByName = new Dictionary<string, int>(columns.Count);
            var columnsById = new Dictionary<int, int>(columns.Count);
            var columnOffsets = new int[columns.Count];
            var columnsByIndex = new ColumnSchema[columns.Count];

            for (int i = 0; i < columns.Count; i++)
            {
                var column = columns[i];

                if (column.Type == DataTypePB.String || column.Type == DataTypePB.Binary)
                {
                    columnOffsets[i] = varLenCnt;
                    varLenCnt++;
                    // Don't increment size here, these types are stored separately
                    // in PartialRow.
                }
                else
                {
                    columnOffsets[i] = size;
                    size += GetTypeSize((KuduType)column.Type);
                }

                hasNulls |= column.IsNullable;
                columnsByName.Add(column.Name, i);
                columnsById.Add((int)column.Id, i);
                columnsByIndex[i] = ColumnSchema.FromProtobuf(columns[i]);

                // TODO: Remove this hack-fix. Kudu throws an exception if columnId is supplied.
                column.ResetId();

                // TODO: store primary key columns
            }

            _columnOffsets = columnOffsets;
            _columnsByName = columnsByName;
            _columnsById = columnsById;
            _columnsByIndex = columnsByIndex;
            RowAllocSize = size;
            VarLengthColumnCount = varLenCnt;
            HasNullableColumns = hasNulls;
        }

        public int ColumnCount => _columnsByIndex.Length;

        public int GetColumnIndex(string name) => _columnsByName[name];

        /// <summary>
        /// If the column is a fixed-length type, the offset is where that
        /// column should be in <see cref="PartialRow._rowAlloc"/>. If the
        /// column is variable-length, the offset is where that column should
        /// be stored in <see cref="PartialRow._varLengthData"/>.
        /// </summary>
        /// <param name="index">The column index.</param>
        public int GetColumnOffset(int index) => _columnOffsets[index];

        public ColumnSchema GetColumn(int index) => _columnsByIndex[index];

        public ColumnSchema GetColumn(string name) => GetColumn(GetColumnIndex(name));

        public int GetColumnIndex(int id) => _columnsById[id];

        public static int GetTypeSize(KuduType type)
        {
            switch (type)
            {
                case KuduType.String:
                case KuduType.Binary:
                    return 8 + 8; // Offset then string length.
                case KuduType.Bool:
                case KuduType.Int8:
                    return 1;
                case KuduType.Int16:
                    return 2;
                case KuduType.Int32:
                case KuduType.Float:
                case KuduType.Decimal32:
                    return 4;
                case KuduType.Int64:
                case KuduType.Double:
                case KuduType.UnixtimeMicros:
                case KuduType.Decimal64:
                    return 8;
                //case DataType.Int128: Not supported in Kudu yet.
                case KuduType.Decimal128:
                    return 16;
                default:
                    throw new ArgumentException();
            }
        }

        public static bool IsSigned(KuduType type)
        {
            switch (type)
            {
                case KuduType.Int8:
                case KuduType.Int16:
                case KuduType.Int32:
                case KuduType.Int64:
                    //case DataType.Int128: Not supported in Kudu yet.
                    return true;
                default:
                    return false;
            }
        }
    }
}
