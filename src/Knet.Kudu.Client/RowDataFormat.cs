using Knet.Kudu.Client.Protobuf.Tserver;

namespace Knet.Kudu.Client
{
    public enum RowDataFormat
    {
        /// <summary>
        /// Server is expected to return scanner result data in row-wise layout.
        /// This is currently the default layout.
        /// </summary>
        Rowwise = RowFormatFlags.NoFlags,
        /// <summary>
        /// Server is expected to return scanner result data in columnar layout.
        /// This layout is more efficient in processing and bandwidth for both
        /// server and client side. It requires server support (kudu-1.12.0 and later),
        /// if it's not supported server still returns data in row-wise layout.
        /// </summary>
        Columnar = RowFormatFlags.ColumnarLayout
    }
}
