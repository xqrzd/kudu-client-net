using Kudu.Client.Protocol.Rpc;
using Kudu.Client.Requests;

namespace Kudu.Client.Connection
{
    public sealed class ParserContext
    {
        public ParseStep Step;

        /// <summary>
        /// Total message length (4 bytes).
        /// </summary>
        public int TotalMessageLength;

        /// <summary>
        /// RPC Header protobuf length (variable encoding).
        /// </summary>
        public int HeaderLength;

        /// <summary>
        /// Main message length (variable encoding).
        /// Includes the size of any sidecars.
        /// </summary>
        public int MainMessageLength;

        /// <summary>
        /// RPC Header protobuf.
        /// </summary>
        public ResponseHeader Header;

        public KuduRpc Rpc;

        public ErrorStatusPB Error;

        /// <summary>
        /// Gets the size of the main message protobuf.
        /// </summary>
        public int ProtobufMessageLength => Header.SidecarOffsets == null ?
            MainMessageLength : (int)Header.SidecarOffsets[0];

        public bool HasSidecars => Header.SidecarOffsets != null;

        public int SidecarLength => MainMessageLength - (int)Header.SidecarOffsets[0];

        public int RemainingSidecarLength;

        public void Reset()
        {
            Step = ParseStep.NotStarted;
            TotalMessageLength = default;
            HeaderLength = default;
            MainMessageLength = default;
            Header = default;
            Rpc = default;
            Error = default;
        }
    }

    public enum ParseStep
    {
        NotStarted,
        ReadTotalMessageLength,
        ReadHeaderLength,
        ReadHeader,
        ReadMainMessageLength,
        ReadProtobufMessage,
        BeginSidecars,
        ReadSidecars
    }
}
