using System;
using System.Collections.Generic;
using Kudu.Client.Protocol;
using Kudu.Client.Protocol.Master;

namespace Kudu.Client
{
    public class Schema
    {
        public GetTableSchemaResponsePB TableSchema { get; }

        // TODO: Create a managed class for this?
        private readonly SchemaPB _schema;
        private readonly Dictionary<string, int> _columnsByName;
        private readonly Dictionary<int, int> _columnsById;
        private readonly int[] _columnOffsets;

        public int ColumnCount { get; }

        public int RowSize { get; }

        public int VarLengthColumnCount { get; }

        public bool HasNullableColumns { get; }

        public Schema(SchemaPB schema, GetTableSchemaResponsePB tableSchema)
        {
            TableSchema = tableSchema;
            var columns = schema.Columns;

            var size = 0;
            var varLenCnt = 0;
            var hasNulls = false;
            var columnOffsets = new int[columns.Count];
            var columnsByName = new Dictionary<string, int>(columns.Count);
            var columnsById = new Dictionary<int, int>(columns.Count);

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
                columnsById.Add((int)column.Id, i);

                // TODO: Remove this hack-fix. Kudu throws an exception if columnId is supplied.
                column.ResetId();

                // TODO: store primary key columns
            }

            _schema = schema;
            _columnOffsets = columnOffsets;
            _columnsByName = columnsByName;
            _columnsById = columnsById;
            ColumnCount = columns.Count;
            RowSize = size;
            VarLengthColumnCount = varLenCnt;
            HasNullableColumns = hasNulls;
        }

        public int GetColumnIndex(string name) => _columnsByName[name];

        public int GetColumnOffset(int index) => _columnOffsets[index];

        public int GetColumnSize(int index) => GetTypeSize(GetColumnType(index));

        public DataType GetColumnType(int index) => _schema.Columns[index].Type;

        public bool IsPrimaryKey(int index) => _schema.Columns[index].IsKey;

        public int GetColumnIndex(int id) => _columnsById[id];

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

        public static bool IsSigned(DataType type)
        {
            switch (type)
            {
                case DataType.Int8:
                case DataType.Int16:
                case DataType.Int32:
                case DataType.Int64:
                case DataType.Int128:
                    return true;
                default:
                    return false;
            }
        }
    }
}
