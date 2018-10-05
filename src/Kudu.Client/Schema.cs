using System;
using System.Collections.Generic;
using Kudu.Client.Protocol;

namespace Kudu.Client
{
    public class Schema
    {
        // TODO: Create a managed class for this?
        private readonly SchemaPB _schema;
        private readonly Dictionary<string, int> _columnsByName;
        private readonly int[] _columnOffsets;

        public int ColumnCount { get; }

        public int RowSize { get; }

        public int VarLengthColumnCount { get; }

        public bool HasNullableColumns { get; }

        public Schema(SchemaPB schema)
        {
            var columns = schema.Columns;

            var size = 0;
            var varLenCnt = 0;
            var hasNulls = false;
            var columnOffsets = new int[columns.Count];
            var columnsByName = new Dictionary<string, int>(columns.Count);

            for (int i = 0; i < columns.Count; i++)
            {
                var column = columns[i];

                if (column.Type == DataType.String || column.Type == DataType.Binary)
                {
                    varLenCnt++;
                    // Don't increment size here, these types are stored separately
                    // in PartialRow.
                }
                else
                {
                    columnOffsets[i] = size;
                    size += GetTypeSize(column.Type);
                }

                hasNulls |= column.IsNullable;
                columnsByName.Add(column.Name, i);

                // TODO: store primary key columns
            }

            _schema = schema;
            _columnOffsets = columnOffsets;
            _columnsByName = columnsByName;
            ColumnCount = columns.Count;
            RowSize = size;
            VarLengthColumnCount = varLenCnt;
            HasNullableColumns = hasNulls;
        }

        public int GetColumnIndex(string name) => _columnsByName[name];

        public int GetColumnOffset(int index) => _columnOffsets[index];

        public int GetColumnSize(int index) => GetTypeSize(GetColumnType(index));

        public DataType GetColumnType(int index) => _schema.Columns[index].Type;

        public static int GetTypeSize(DataType type)
        {
            switch (type)
            {
                case DataType.String:
                case DataType.Binary:
                    return 8 + 8; // offset then string length
                case DataType.Bool:
                case DataType.Int8:
                case DataType.Uint8:
                    return 1;
                case DataType.Int16:
                case DataType.Uint16:
                    return 2;
                case DataType.Int32:
                case DataType.Uint32:
                case DataType.Float:
                case DataType.Decimal32:
                    return 4;
                case DataType.Int64:
                case DataType.Uint64:
                case DataType.Double:
                case DataType.UnixtimeMicros:
                case DataType.Decimal64:
                    return 8;
                case DataType.Int128:
                case DataType.Decimal128:
                    return 16;
                default:
                    throw new ArgumentException();
            }
        }
    }
}
