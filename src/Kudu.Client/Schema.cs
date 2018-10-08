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

                if (column.Type == DataTypePB.String || column.Type == DataTypePB.Binary)
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

        public DataTypePB GetColumnType(int index) => _schema.Columns[index].Type;

        public bool IsPrimaryKey(int index) => _schema.Columns[index].IsKey;

        public int GetColumnIndex(int id) => _columnsById[id];

        public static int GetTypeSize(DataTypePB type)
        {
            switch (type)
            {
                case DataTypePB.String:
                case DataTypePB.Binary:
                    return 8 + 8; // offset then string length
                case DataTypePB.Bool:
                case DataTypePB.Int8:
                case DataTypePB.Uint8:
                    return 1;
                case DataTypePB.Int16:
                case DataTypePB.Uint16:
                    return 2;
                case DataTypePB.Int32:
                case DataTypePB.Uint32:
                case DataTypePB.Float:
                case DataTypePB.Decimal32:
                    return 4;
                case DataTypePB.Int64:
                case DataTypePB.Uint64:
                case DataTypePB.Double:
                case DataTypePB.UnixtimeMicros:
                case DataTypePB.Decimal64:
                    return 8;
                case DataTypePB.Int128:
                case DataTypePB.Decimal128:
                    return 16;
                default:
                    throw new ArgumentException();
            }
        }

        public static bool IsSigned(DataTypePB type)
        {
            switch (type)
            {
                case DataTypePB.Int8:
                case DataTypePB.Int16:
                case DataTypePB.Int32:
                case DataTypePB.Int64:
                case DataTypePB.Int128:
                    return true;
                default:
                    return false;
            }
        }
    }
}
