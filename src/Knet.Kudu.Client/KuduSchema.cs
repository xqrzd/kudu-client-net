using System;
using System.Collections.Generic;
using Knet.Kudu.Client.Protobuf;
using Knet.Kudu.Client.Util;

namespace Knet.Kudu.Client
{
    /// <summary>
    /// Represents table's schema which is essentially a list of columns.
    /// </summary>
    public class KuduSchema
    {
        /// <summary>
        /// Maps column index to column.
        /// </summary>
        private readonly ColumnSchema[] _columnsByIndex;

        /// <summary>
        /// The primary key columns.
        /// </summary>
        private readonly List<ColumnSchema> _primaryKeyColumns;

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

        public bool HasNullableColumns { get; }

        /// <summary>
        /// Index of the IS_DELETED virtual column.
        /// </summary>
        public int IsDeletedIndex { get; }

        /// <summary>
        /// The size of all fixed-length columns.
        /// </summary>
        internal int RowAllocSize { get; }

        /// <summary>
        /// Get the size a row built using this schema would be.
        /// </summary>
        internal int RowSize { get; }

        internal int VarLengthColumnCount { get; }

        public KuduSchema(List<ColumnSchema> columns, int isDeletedIndex = -1)
            : this(columns, null, isDeletedIndex) { }

        public KuduSchema(List<ColumnSchema> columns, List<int> columnIds, int isDeletedIndex = -1)
        {
            var hasColumnIds = columnIds != null;
            if (hasColumnIds)
                _columnsById = new Dictionary<int, int>(columns.Count);

            _primaryKeyColumns = new List<ColumnSchema>();
            _columnsByName = new Dictionary<string, int>(columns.Count);
            _columnsById = new Dictionary<int, int>(columns.Count);
            _columnOffsets = new int[columns.Count];
            IsDeletedIndex = isDeletedIndex;

            for (int i = 0; i < columns.Count; i++)
            {
                var column = columns[i];

                if (column.IsFixedSize)
                {
                    _columnOffsets[i] = RowAllocSize;
                    RowAllocSize += column.Size;
                }
                else
                {
                    _columnOffsets[i] = VarLengthColumnCount;
                    VarLengthColumnCount++;

                    // Don't increment size here, these types are stored separately
                    // in PartialRow (_varLengthData).
                }

                HasNullableColumns |= column.IsNullable;
                _columnsByName.Add(column.Name, i);

                if (hasColumnIds)
                    _columnsById.Add(columnIds[i], i);

                if (column.IsKey)
                    _primaryKeyColumns.Add(column);
            }

            _columnsByIndex = new ColumnSchema[columns.Count];
            columns.CopyTo(_columnsByIndex);
            RowSize = GetRowSize(_columnsByIndex);
        }

        public KuduSchema(SchemaPB schema)
        {
            var columns = schema.Columns;

            var size = 0;
            var varLenCnt = 0;
            var hasNulls = false;
            var primaryKeyColumns = new List<ColumnSchema>();
            var columnsByName = new Dictionary<string, int>(columns.Count);
            var columnsById = new Dictionary<int, int>(columns.Count);
            var columnOffsets = new int[columns.Count];
            var columnsByIndex = new ColumnSchema[columns.Count];
            var isDeletedIndex = -1;

            for (int i = 0; i < columns.Count; i++)
            {
                var column = columns[i];

                if (column.Type == DataTypePB.String ||
                    column.Type == DataTypePB.Binary ||
                    column.Type == DataTypePB.Varchar)
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

                if (column.IsKey)
                    primaryKeyColumns.Add(columnsByIndex[i]);

                if (column.Type == DataTypePB.IsDeleted)
                    isDeletedIndex = i;
            }

            _columnOffsets = columnOffsets;
            _columnsByName = columnsByName;
            _columnsById = columnsById;
            _columnsByIndex = columnsByIndex;
            _primaryKeyColumns = primaryKeyColumns;
            RowAllocSize = size;
            VarLengthColumnCount = varLenCnt;
            HasNullableColumns = hasNulls;
            RowSize = GetRowSize(columnsByIndex);
            IsDeletedIndex = isDeletedIndex;
        }

        public IReadOnlyList<ColumnSchema> Columns => _columnsByIndex;

        internal int PrimaryKeyColumnCount => _primaryKeyColumns.Count;

        /// <summary>
        /// Tells whether this schema includes IDs for columns. A schema created by a
        /// client as part of table creation will not include IDs, but schemas for open
        /// tables will include IDs.
        /// </summary>
        public bool HasColumnIds => _columnsById != null;

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

        /// <summary>
        /// Returns true if the column exists.
        /// </summary>
        /// <param name="columnName">Column to search for.</param>
        public bool HasColumn(string columnName) => _columnsByName.ContainsKey(columnName);

        /// <summary>
        /// True if the schema has the IS_DELETED virtual column.
        /// </summary>
        public bool HasIsDeleted => IsDeletedIndex != -1;

        public static int GetTypeSize(KuduType type)
        {
            switch (type)
            {
                case KuduType.String:
                case KuduType.Binary:
                case KuduType.Varchar:
                    return 8 + 8; // Offset then string length.
                case KuduType.Bool:
                case KuduType.Int8:
                    return 1;
                case KuduType.Int16:
                    return 2;
                case KuduType.Int32:
                case KuduType.Float:
                case KuduType.Decimal32:
                case KuduType.Date:
                    return 4;
                case KuduType.Int64:
                case KuduType.Double:
                case KuduType.UnixtimeMicros:
                case KuduType.Decimal64:
                    return 8;
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
                case KuduType.Date:
                case KuduType.UnixtimeMicros:
                case KuduType.Decimal32:
                case KuduType.Decimal64:
                case KuduType.Decimal128:
                    return true;
                default:
                    return false;
            }
        }

        /// <summary>
        /// Gives the size in bytes for a single row given the specified schema
        /// </summary>
        /// <param name="columns">The row's columns.</param>
        private static int GetRowSize(ColumnSchema[] columns)
        {
            int totalSize = 0;
            bool hasNullables = false;
            foreach (ColumnSchema column in columns)
            {
                totalSize += column.Size;
                hasNullables |= column.IsNullable;
            }
            if (hasNullables)
            {
                totalSize += KuduEncoder.BitsToBytes(columns.Length);
            }
            return totalSize;
        }
    }
}
