using Kudu.Client.Protocol.Rpc;

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
        public uint HeaderLength;

        /// <summary>
        /// Main message length (variable encoding).
        /// Includes the size of any sidecars.
        /// </summary>
        public uint MainMessageLength;

        /// <summary>
        /// RPC Header protobuf.
        /// </summary>
        public ResponseHeader Header;

        /// <summary>
        /// Gets the size of the main message protobuf.
        /// </summary>
        public int ProtobufMessageLength => Header.SidecarOffsets == null ?
            (int)MainMessageLength : (int)Header.SidecarOffsets[0];

        public bool HasSidecars => Header.SidecarOffsets != null;

        public int SidecarLength => (int)MainMessageLength - (int)Header.SidecarOffsets[0];
    }

    public enum ParseStep
    {
        NotStarted,
        ReadTotalMessageLength,
        ReadHeaderLength,
        ReadHeader,
        ReadMainMessageLength,
        ReadProtobufMessage
    }
}
