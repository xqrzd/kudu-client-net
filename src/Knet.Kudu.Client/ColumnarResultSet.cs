using System;
using System.Runtime.CompilerServices;
using Knet.Kudu.Client.Connection;
using Knet.Kudu.Client.Protocol;

namespace Knet.Kudu.Client
{
    public sealed class ColumnarResultSet : IDisposable
    {
        private readonly KuduSidecars _sidecars;
        private readonly int[] _dataSidecarOffsets;
        private readonly int[] _varlenDataSidecarOffsets;
        private readonly int[] _nonNullBitmapSidecarOffsets;

        public KuduSchema Schema { get; }

        public long Count { get; }

        public ColumnarResultSet(
            KuduSidecars sidecars,
            KuduSchema schema,
            ColumnarRowBlockPB rowBlockPb)
        {
            _sidecars = sidecars;
            Schema = schema;
            Count = rowBlockPb.NumRows;

            if (sidecars is null)
            {
                // Empty projection.
                return;
            }

            var columns = rowBlockPb.Columns;
            var numColumns = columns.Count;

            var dataSidecarOffsets = new int[numColumns];
            var varlenDataSidecarOffsets = new int[numColumns];
            var nonNullBitmapSidecarOffsets = new int[numColumns];

            for (int i = 0; i < numColumns; i++)
            {
                var column = columns[i];

                if (column.ShouldSerializeDataSidecar())
                {
                    var offset = sidecars.GetOffset(column.DataSidecar);
                    dataSidecarOffsets[i] = offset;
                }

                if (column.ShouldSerializeVarlenDataSidecar())
                {
                    var offset = sidecars.GetOffset(column.VarlenDataSidecar);
                    varlenDataSidecarOffsets[i] = offset;
                }

                if (column.ShouldSerializeNonNullBitmapSidecar())
                {
                    var offset = sidecars.GetOffset(column.NonNullBitmapSidecar);
                    nonNullBitmapSidecarOffsets[i] = offset;
                }
                else
                {
                    nonNullBitmapSidecarOffsets[i] = -1;
                }
            }

            _dataSidecarOffsets = dataSidecarOffsets;
            _varlenDataSidecarOffsets = varlenDataSidecarOffsets;
            _nonNullBitmapSidecarOffsets = nonNullBitmapSidecarOffsets;
        }

        public void Dispose()
        {
            _sidecars?.Dispose();
        }

        internal int GetDataOffset(int columnIndex) =>
            _dataSidecarOffsets[columnIndex];

        internal int GetVarLenOffset(int columnIndex) =>
            _varlenDataSidecarOffsets[columnIndex];

        internal int GetNonNullBitmapOffset(int columnIndex) =>
            _nonNullBitmapSidecarOffsets[columnIndex];

        private ReadOnlySpan<byte> GetData()
        {
            var sidecars = _sidecars;

            if (sidecars is null)
            {
                // Empty projection.
                return default;
            }
            else
            {
                return sidecars.Span;
            }
        }

        public override string ToString() => $"{Count} rows";

        public Enumerator GetEnumerator() => new Enumerator(this);

        public ref struct Enumerator
        {
            private readonly ColumnarResultSet _resultSet;
            private readonly ReadOnlySpan<byte> _data;
            private readonly int _numRows;
            private int _index;

            internal Enumerator(ColumnarResultSet resultSet)
            {
                _resultSet = resultSet;
                _index = -1;
                _numRows = (int)resultSet.Count;
                _data = resultSet.GetData();
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool MoveNext()
            {
                int index = _index + 1;
                if (index < _numRows)
                {
                    _index = index;
                    return true;
                }

                return false;
            }

            public ColumnarRowResult Current
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get
                {
                    return new ColumnarRowResult(_resultSet, _data, _index);
                }
            }
        }
    }
}
