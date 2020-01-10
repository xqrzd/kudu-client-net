using Knet.Kudu.Client.Protocol;

namespace Knet.Kudu.Client
{
    public class ColumnBuilder
    {
        internal ColumnSchemaPB Column;

        public ColumnBuilder()
        {
            Column = new ColumnSchemaPB();
        }

        public string Name
        {
            get => Column.Name;
            set => Column.Name = value;
        }

        public KuduType Type
        {
            get => (KuduType)Column.Type;
            set => Column.Type = (DataTypePB)value;
        }

        public bool IsKey
        {
            get => Column.IsKey;
            set => Column.IsKey = value;
        }

        public bool IsNullable
        {
            get => Column.IsNullable;
            set => Column.IsNullable = value;
        }

        public byte[] ReadDefaultValue
        {
            get => Column.ReadDefaultValue;
            set => Column.ReadDefaultValue = value;
        }

        public byte[] WriteDefaultValue
        {
            get => Column.WriteDefaultValue;
            set => Column.WriteDefaultValue = value;
        }

        public EncodingType Encoding
        {
            get => (EncodingType)Column.Encoding;
            set => Column.Encoding = (EncodingTypePB)value;
        }

        public CompressionType Compression
        {
            get => (CompressionType)Column.Compression;
            set => Column.Compression = (CompressionTypePB)value;
        }

        public int CfileBlockSize
        {
            get => Column.CfileBlockSize;
            set => Column.CfileBlockSize = value;
        }

        public int Precision
        {
            get
            {
                InitTypeAttributes();
                return Column.TypeAttributes.Precision;
            }
            set
            {
                InitTypeAttributes();
                Column.TypeAttributes.Precision = value;
            }
        }

        public int Scale
        {
            get
            {
                InitTypeAttributes();
                return Column.TypeAttributes.Scale;
            }
            set
            {
                InitTypeAttributes();
                Column.TypeAttributes.Scale = value;
            }
        }

        private void InitTypeAttributes()
        {
            if (Column.TypeAttributes is null)
                Column.TypeAttributes = new ColumnTypeAttributesPB();
        }

        public static implicit operator ColumnSchemaPB(ColumnBuilder builder) => builder.Column;
    }
}
