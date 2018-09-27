// This file was generated by a tool; you should avoid making direct changes.
// Consider using 'partial classes' to extend these types
// Input: tablet_copy.proto

#pragma warning disable CS1591, CS0612, CS3021, IDE1006
namespace Kudu.Client.Protocol.Tserver
{

    [global::ProtoBuf.ProtoContract()]
    public partial class TabletCopyErrorPB : global::ProtoBuf.IExtensible
    {
        private global::ProtoBuf.IExtension __pbn__extensionData;
        global::ProtoBuf.IExtension global::ProtoBuf.IExtensible.GetExtensionObject(bool createIfMissing)
            => global::ProtoBuf.Extensible.GetExtensionObject(ref __pbn__extensionData, createIfMissing);

        [global::ProtoBuf.ProtoMember(1, IsRequired = true)]
        public Code code { get; set; } = Code.UnknownError;

        [global::ProtoBuf.ProtoMember(2, Name = @"status", IsRequired = true)]
        public global::Kudu.Client.Protocol.AppStatusPB Status { get; set; }

        [global::ProtoBuf.ProtoContract()]
        public enum Code
        {
            [global::ProtoBuf.ProtoEnum(Name = @"UNKNOWN_ERROR")]
            UnknownError = 1,
            [global::ProtoBuf.ProtoEnum(Name = @"NO_SESSION")]
            NoSession = 2,
            [global::ProtoBuf.ProtoEnum(Name = @"TABLET_NOT_FOUND")]
            TabletNotFound = 3,
            [global::ProtoBuf.ProtoEnum(Name = @"BLOCK_NOT_FOUND")]
            BlockNotFound = 4,
            [global::ProtoBuf.ProtoEnum(Name = @"WAL_SEGMENT_NOT_FOUND")]
            WalSegmentNotFound = 5,
            [global::ProtoBuf.ProtoEnum(Name = @"INVALID_TABLET_COPY_REQUEST")]
            InvalidTabletCopyRequest = 6,
            [global::ProtoBuf.ProtoEnum(Name = @"IO_ERROR")]
            IoError = 7,
        }

        public static class Extensions
        {
            public static TabletCopyErrorPB GetTabletCopyErrorExt(global::Kudu.Client.Protocol.Rpc.ErrorStatusPB obj)
                => obj == null ? default : global::ProtoBuf.Extensible.GetValue<TabletCopyErrorPB>(obj, 102);

            public static void SetTabletCopyErrorExt(global::Kudu.Client.Protocol.Rpc.ErrorStatusPB obj, TabletCopyErrorPB value)
                => global::ProtoBuf.Extensible.AppendValue<TabletCopyErrorPB>(obj, 102, value);

        }
    }

    [global::ProtoBuf.ProtoContract()]
    public partial class BeginTabletCopySessionRequestPB : global::ProtoBuf.IExtensible
    {
        private global::ProtoBuf.IExtension __pbn__extensionData;
        global::ProtoBuf.IExtension global::ProtoBuf.IExtensible.GetExtensionObject(bool createIfMissing)
            => global::ProtoBuf.Extensible.GetExtensionObject(ref __pbn__extensionData, createIfMissing);

        [global::ProtoBuf.ProtoMember(1, Name = @"requestor_uuid", IsRequired = true)]
        public byte[] RequestorUuid { get; set; }

        [global::ProtoBuf.ProtoMember(2, Name = @"tablet_id", IsRequired = true)]
        public byte[] TabletId { get; set; }

    }

    [global::ProtoBuf.ProtoContract()]
    public partial class BeginTabletCopySessionResponsePB : global::ProtoBuf.IExtensible
    {
        private global::ProtoBuf.IExtension __pbn__extensionData;
        global::ProtoBuf.IExtension global::ProtoBuf.IExtensible.GetExtensionObject(bool createIfMissing)
            => global::ProtoBuf.Extensible.GetExtensionObject(ref __pbn__extensionData, createIfMissing);

        [global::ProtoBuf.ProtoMember(1, Name = @"session_id", IsRequired = true)]
        public byte[] SessionId { get; set; }

        [global::ProtoBuf.ProtoMember(2, Name = @"session_idle_timeout_millis", IsRequired = true)]
        public ulong SessionIdleTimeoutMillis { get; set; }

        [global::ProtoBuf.ProtoMember(3, Name = @"superblock", IsRequired = true)]
        public global::Kudu.Client.Protocol.Tablet.TabletSuperBlockPB Superblock { get; set; }

        [global::ProtoBuf.ProtoMember(4, Name = @"wal_segment_seqnos")]
        public ulong[] WalSegmentSeqnos { get; set; }

        [global::ProtoBuf.ProtoMember(5, Name = @"initial_cstate", IsRequired = true)]
        public global::Kudu.Client.Protocol.Consensus.ConsensusStatePB InitialCstate { get; set; }

        [global::ProtoBuf.ProtoMember(6, Name = @"responder_uuid")]
        public byte[] ResponderUuid
        {
            get { return __pbn__ResponderUuid; }
            set { __pbn__ResponderUuid = value; }
        }
        public bool ShouldSerializeResponderUuid() => __pbn__ResponderUuid != null;
        public void ResetResponderUuid() => __pbn__ResponderUuid = null;
        private byte[] __pbn__ResponderUuid;

    }

    [global::ProtoBuf.ProtoContract()]
    public partial class CheckTabletCopySessionActiveRequestPB : global::ProtoBuf.IExtensible
    {
        private global::ProtoBuf.IExtension __pbn__extensionData;
        global::ProtoBuf.IExtension global::ProtoBuf.IExtensible.GetExtensionObject(bool createIfMissing)
            => global::ProtoBuf.Extensible.GetExtensionObject(ref __pbn__extensionData, createIfMissing);

        [global::ProtoBuf.ProtoMember(1, Name = @"session_id", IsRequired = true)]
        public byte[] SessionId { get; set; }

        [global::ProtoBuf.ProtoMember(2, Name = @"keepalive")]
        [global::System.ComponentModel.DefaultValue(false)]
        public bool Keepalive
        {
            get { return __pbn__Keepalive ?? false; }
            set { __pbn__Keepalive = value; }
        }
        public bool ShouldSerializeKeepalive() => __pbn__Keepalive != null;
        public void ResetKeepalive() => __pbn__Keepalive = null;
        private bool? __pbn__Keepalive;

    }

    [global::ProtoBuf.ProtoContract()]
    public partial class CheckTabletCopySessionActiveResponsePB : global::ProtoBuf.IExtensible
    {
        private global::ProtoBuf.IExtension __pbn__extensionData;
        global::ProtoBuf.IExtension global::ProtoBuf.IExtensible.GetExtensionObject(bool createIfMissing)
            => global::ProtoBuf.Extensible.GetExtensionObject(ref __pbn__extensionData, createIfMissing);

        [global::ProtoBuf.ProtoMember(1, Name = @"session_is_active", IsRequired = true)]
        public bool SessionIsActive { get; set; }

    }

    [global::ProtoBuf.ProtoContract()]
    public partial class DataIdPB : global::ProtoBuf.IExtensible
    {
        private global::ProtoBuf.IExtension __pbn__extensionData;
        global::ProtoBuf.IExtension global::ProtoBuf.IExtensible.GetExtensionObject(bool createIfMissing)
            => global::ProtoBuf.Extensible.GetExtensionObject(ref __pbn__extensionData, createIfMissing);

        [global::ProtoBuf.ProtoMember(1, Name = @"type", IsRequired = true)]
        public IdType Type { get; set; }

        [global::ProtoBuf.ProtoMember(2, Name = @"block_id")]
        public global::Kudu.Client.Protocol.BlockIdPB BlockId { get; set; }

        [global::ProtoBuf.ProtoMember(3, Name = @"wal_segment_seqno")]
        public ulong WalSegmentSeqno
        {
            get { return __pbn__WalSegmentSeqno.GetValueOrDefault(); }
            set { __pbn__WalSegmentSeqno = value; }
        }
        public bool ShouldSerializeWalSegmentSeqno() => __pbn__WalSegmentSeqno != null;
        public void ResetWalSegmentSeqno() => __pbn__WalSegmentSeqno = null;
        private ulong? __pbn__WalSegmentSeqno;

        [global::ProtoBuf.ProtoContract()]
        public enum IdType
        {
            [global::ProtoBuf.ProtoEnum(Name = @"UNKNOWN")]
            Unknown = 0,
            [global::ProtoBuf.ProtoEnum(Name = @"BLOCK")]
            Block = 1,
            [global::ProtoBuf.ProtoEnum(Name = @"LOG_SEGMENT")]
            LogSegment = 2,
        }

    }

    [global::ProtoBuf.ProtoContract()]
    public partial class FetchDataRequestPB : global::ProtoBuf.IExtensible
    {
        private global::ProtoBuf.IExtension __pbn__extensionData;
        global::ProtoBuf.IExtension global::ProtoBuf.IExtensible.GetExtensionObject(bool createIfMissing)
            => global::ProtoBuf.Extensible.GetExtensionObject(ref __pbn__extensionData, createIfMissing);

        [global::ProtoBuf.ProtoMember(1, Name = @"session_id", IsRequired = true)]
        public byte[] SessionId { get; set; }

        [global::ProtoBuf.ProtoMember(2, Name = @"data_id", IsRequired = true)]
        public DataIdPB DataId { get; set; }

        [global::ProtoBuf.ProtoMember(3, Name = @"offset")]
        [global::System.ComponentModel.DefaultValue(0)]
        public ulong Offset
        {
            get { return __pbn__Offset ?? 0; }
            set { __pbn__Offset = value; }
        }
        public bool ShouldSerializeOffset() => __pbn__Offset != null;
        public void ResetOffset() => __pbn__Offset = null;
        private ulong? __pbn__Offset;

        [global::ProtoBuf.ProtoMember(4, Name = @"max_length")]
        [global::System.ComponentModel.DefaultValue(0)]
        public long MaxLength
        {
            get { return __pbn__MaxLength ?? 0; }
            set { __pbn__MaxLength = value; }
        }
        public bool ShouldSerializeMaxLength() => __pbn__MaxLength != null;
        public void ResetMaxLength() => __pbn__MaxLength = null;
        private long? __pbn__MaxLength;

    }

    [global::ProtoBuf.ProtoContract()]
    public partial class DataChunkPB : global::ProtoBuf.IExtensible
    {
        private global::ProtoBuf.IExtension __pbn__extensionData;
        global::ProtoBuf.IExtension global::ProtoBuf.IExtensible.GetExtensionObject(bool createIfMissing)
            => global::ProtoBuf.Extensible.GetExtensionObject(ref __pbn__extensionData, createIfMissing);

        [global::ProtoBuf.ProtoMember(1, Name = @"offset", IsRequired = true)]
        public ulong Offset { get; set; }

        [global::ProtoBuf.ProtoMember(2, Name = @"data", IsRequired = true)]
        public byte[] Data { get; set; }

        [global::ProtoBuf.ProtoMember(3, Name = @"crc32", DataFormat = global::ProtoBuf.DataFormat.FixedSize, IsRequired = true)]
        public uint Crc32 { get; set; }

        [global::ProtoBuf.ProtoMember(4, Name = @"total_data_length", IsRequired = true)]
        public long TotalDataLength { get; set; }

    }

    [global::ProtoBuf.ProtoContract()]
    public partial class FetchDataResponsePB : global::ProtoBuf.IExtensible
    {
        private global::ProtoBuf.IExtension __pbn__extensionData;
        global::ProtoBuf.IExtension global::ProtoBuf.IExtensible.GetExtensionObject(bool createIfMissing)
            => global::ProtoBuf.Extensible.GetExtensionObject(ref __pbn__extensionData, createIfMissing);

        [global::ProtoBuf.ProtoMember(1, Name = @"chunk", IsRequired = true)]
        public DataChunkPB Chunk { get; set; }

    }

    [global::ProtoBuf.ProtoContract()]
    public partial class EndTabletCopySessionRequestPB : global::ProtoBuf.IExtensible
    {
        private global::ProtoBuf.IExtension __pbn__extensionData;
        global::ProtoBuf.IExtension global::ProtoBuf.IExtensible.GetExtensionObject(bool createIfMissing)
            => global::ProtoBuf.Extensible.GetExtensionObject(ref __pbn__extensionData, createIfMissing);

        [global::ProtoBuf.ProtoMember(1, Name = @"session_id", IsRequired = true)]
        public byte[] SessionId { get; set; }

        [global::ProtoBuf.ProtoMember(2, Name = @"is_success", IsRequired = true)]
        public bool IsSuccess { get; set; }

        [global::ProtoBuf.ProtoMember(3, Name = @"error")]
        public global::Kudu.Client.Protocol.AppStatusPB Error { get; set; }

    }

    [global::ProtoBuf.ProtoContract()]
    public partial class EndTabletCopySessionResponsePB : global::ProtoBuf.IExtensible
    {
        private global::ProtoBuf.IExtension __pbn__extensionData;
        global::ProtoBuf.IExtension global::ProtoBuf.IExtensible.GetExtensionObject(bool createIfMissing)
            => global::ProtoBuf.Extensible.GetExtensionObject(ref __pbn__extensionData, createIfMissing);

    }

}

#pragma warning restore CS1591, CS0612, CS3021, IDE1006