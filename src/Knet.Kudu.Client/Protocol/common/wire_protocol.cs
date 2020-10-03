// This file was generated by a tool; you should avoid making direct changes.
// Consider using 'partial classes' to extend these types
// Input: wire_protocol.proto

#pragma warning disable CS1591, CS0612, CS3021, IDE1006
namespace Knet.Kudu.Client.Protocol
{

    [global::ProtoBuf.ProtoContract()]
    public partial class AppStatusPB : global::ProtoBuf.IExtensible
    {
        private global::ProtoBuf.IExtension __pbn__extensionData;
        global::ProtoBuf.IExtension global::ProtoBuf.IExtensible.GetExtensionObject(bool createIfMissing)
            => global::ProtoBuf.Extensible.GetExtensionObject(ref __pbn__extensionData, createIfMissing);

        [global::ProtoBuf.ProtoMember(1, Name = @"code", IsRequired = true)]
        public ErrorCode Code { get; set; } = ErrorCode.UnknownError;

        [global::ProtoBuf.ProtoMember(2, Name = @"message")]
        [global::System.ComponentModel.DefaultValue("")]
        public string Message
        {
            get { return __pbn__Message ?? ""; }
            set { __pbn__Message = value; }
        }
        public bool ShouldSerializeMessage() => __pbn__Message != null;
        public void ResetMessage() => __pbn__Message = null;
        private string __pbn__Message;

        [global::ProtoBuf.ProtoMember(4, Name = @"posix_code")]
        public int PosixCode
        {
            get { return __pbn__PosixCode.GetValueOrDefault(); }
            set { __pbn__PosixCode = value; }
        }
        public bool ShouldSerializePosixCode() => __pbn__PosixCode != null;
        public void ResetPosixCode() => __pbn__PosixCode = null;
        private int? __pbn__PosixCode;

        [global::ProtoBuf.ProtoContract()]
        public enum ErrorCode
        {
            [global::ProtoBuf.ProtoEnum(Name = @"UNKNOWN_ERROR")]
            UnknownError = 999,
            [global::ProtoBuf.ProtoEnum(Name = @"OK")]
            Ok = 0,
            [global::ProtoBuf.ProtoEnum(Name = @"NOT_FOUND")]
            NotFound = 1,
            [global::ProtoBuf.ProtoEnum(Name = @"CORRUPTION")]
            Corruption = 2,
            [global::ProtoBuf.ProtoEnum(Name = @"NOT_SUPPORTED")]
            NotSupported = 3,
            [global::ProtoBuf.ProtoEnum(Name = @"INVALID_ARGUMENT")]
            InvalidArgument = 4,
            [global::ProtoBuf.ProtoEnum(Name = @"IO_ERROR")]
            IoError = 5,
            [global::ProtoBuf.ProtoEnum(Name = @"ALREADY_PRESENT")]
            AlreadyPresent = 6,
            [global::ProtoBuf.ProtoEnum(Name = @"RUNTIME_ERROR")]
            RuntimeError = 7,
            [global::ProtoBuf.ProtoEnum(Name = @"NETWORK_ERROR")]
            NetworkError = 8,
            [global::ProtoBuf.ProtoEnum(Name = @"ILLEGAL_STATE")]
            IllegalState = 9,
            [global::ProtoBuf.ProtoEnum(Name = @"NOT_AUTHORIZED")]
            NotAuthorized = 10,
            [global::ProtoBuf.ProtoEnum(Name = @"ABORTED")]
            Aborted = 11,
            [global::ProtoBuf.ProtoEnum(Name = @"REMOTE_ERROR")]
            RemoteError = 12,
            [global::ProtoBuf.ProtoEnum(Name = @"SERVICE_UNAVAILABLE")]
            ServiceUnavailable = 13,
            [global::ProtoBuf.ProtoEnum(Name = @"TIMED_OUT")]
            TimedOut = 14,
            [global::ProtoBuf.ProtoEnum(Name = @"UNINITIALIZED")]
            Uninitialized = 15,
            [global::ProtoBuf.ProtoEnum(Name = @"CONFIGURATION_ERROR")]
            ConfigurationError = 16,
            [global::ProtoBuf.ProtoEnum(Name = @"INCOMPLETE")]
            Incomplete = 17,
            [global::ProtoBuf.ProtoEnum(Name = @"END_OF_FILE")]
            EndOfFile = 18,
            [global::ProtoBuf.ProtoEnum(Name = @"CANCELLED")]
            Cancelled = 19,
        }

    }

    [global::ProtoBuf.ProtoContract()]
    public partial class NodeInstancePB : global::ProtoBuf.IExtensible
    {
        private global::ProtoBuf.IExtension __pbn__extensionData;
        global::ProtoBuf.IExtension global::ProtoBuf.IExtensible.GetExtensionObject(bool createIfMissing)
            => global::ProtoBuf.Extensible.GetExtensionObject(ref __pbn__extensionData, createIfMissing);

        [global::ProtoBuf.ProtoMember(1, Name = @"permanent_uuid", IsRequired = true)]
        public byte[] PermanentUuid { get; set; }

        [global::ProtoBuf.ProtoMember(2, Name = @"instance_seqno", IsRequired = true)]
        public long InstanceSeqno { get; set; }

    }

    [global::ProtoBuf.ProtoContract()]
    public partial class ServerRegistrationPB : global::ProtoBuf.IExtensible
    {
        private global::ProtoBuf.IExtension __pbn__extensionData;
        global::ProtoBuf.IExtension global::ProtoBuf.IExtensible.GetExtensionObject(bool createIfMissing)
            => global::ProtoBuf.Extensible.GetExtensionObject(ref __pbn__extensionData, createIfMissing);

        [global::ProtoBuf.ProtoMember(1, Name = @"rpc_addresses")]
        public global::System.Collections.Generic.List<HostPortPB> RpcAddresses { get; } = new global::System.Collections.Generic.List<HostPortPB>();

        [global::ProtoBuf.ProtoMember(2, Name = @"http_addresses")]
        public global::System.Collections.Generic.List<HostPortPB> HttpAddresses { get; } = new global::System.Collections.Generic.List<HostPortPB>();

        [global::ProtoBuf.ProtoMember(3, Name = @"software_version")]
        [global::System.ComponentModel.DefaultValue("")]
        public string SoftwareVersion
        {
            get { return __pbn__SoftwareVersion ?? ""; }
            set { __pbn__SoftwareVersion = value; }
        }
        public bool ShouldSerializeSoftwareVersion() => __pbn__SoftwareVersion != null;
        public void ResetSoftwareVersion() => __pbn__SoftwareVersion = null;
        private string __pbn__SoftwareVersion;

        [global::ProtoBuf.ProtoMember(4, Name = @"https_enabled")]
        public bool HttpsEnabled
        {
            get { return __pbn__HttpsEnabled.GetValueOrDefault(); }
            set { __pbn__HttpsEnabled = value; }
        }
        public bool ShouldSerializeHttpsEnabled() => __pbn__HttpsEnabled != null;
        public void ResetHttpsEnabled() => __pbn__HttpsEnabled = null;
        private bool? __pbn__HttpsEnabled;

        [global::ProtoBuf.ProtoMember(5, Name = @"start_time")]
        public long StartTime
        {
            get { return __pbn__StartTime.GetValueOrDefault(); }
            set { __pbn__StartTime = value; }
        }
        public bool ShouldSerializeStartTime() => __pbn__StartTime != null;
        public void ResetStartTime() => __pbn__StartTime = null;
        private long? __pbn__StartTime;

        [global::ProtoBuf.ProtoMember(6, Name = @"unix_domain_socket_path")]
        [global::System.ComponentModel.DefaultValue("")]
        public string UnixDomainSocketPath
        {
            get { return __pbn__UnixDomainSocketPath ?? ""; }
            set { __pbn__UnixDomainSocketPath = value; }
        }
        public bool ShouldSerializeUnixDomainSocketPath() => __pbn__UnixDomainSocketPath != null;
        public void ResetUnixDomainSocketPath() => __pbn__UnixDomainSocketPath = null;
        private string __pbn__UnixDomainSocketPath;

    }

    [global::ProtoBuf.ProtoContract()]
    public partial class ServerEntryPB : global::ProtoBuf.IExtensible
    {
        private global::ProtoBuf.IExtension __pbn__extensionData;
        global::ProtoBuf.IExtension global::ProtoBuf.IExtensible.GetExtensionObject(bool createIfMissing)
            => global::ProtoBuf.Extensible.GetExtensionObject(ref __pbn__extensionData, createIfMissing);

        [global::ProtoBuf.ProtoMember(1, Name = @"error")]
        public AppStatusPB Error { get; set; }

        [global::ProtoBuf.ProtoMember(2, Name = @"instance_id")]
        public NodeInstancePB InstanceId { get; set; }

        [global::ProtoBuf.ProtoMember(3, Name = @"registration")]
        public ServerRegistrationPB Registration { get; set; }

        [global::ProtoBuf.ProtoMember(4, Name = @"role")]
        [global::System.ComponentModel.DefaultValue(global::Knet.Kudu.Client.Protocol.Consensus.RaftPeerPB.Role.UnknownRole)]
        public global::Knet.Kudu.Client.Protocol.Consensus.RaftPeerPB.Role Role
        {
            get { return __pbn__Role ?? global::Knet.Kudu.Client.Protocol.Consensus.RaftPeerPB.Role.UnknownRole; }
            set { __pbn__Role = value; }
        }
        public bool ShouldSerializeRole() => __pbn__Role != null;
        public void ResetRole() => __pbn__Role = null;
        private global::Knet.Kudu.Client.Protocol.Consensus.RaftPeerPB.Role? __pbn__Role;

    }

    [global::ProtoBuf.ProtoContract()]
    public partial class RowwiseRowBlockPB : global::ProtoBuf.IExtensible
    {
        private global::ProtoBuf.IExtension __pbn__extensionData;
        global::ProtoBuf.IExtension global::ProtoBuf.IExtensible.GetExtensionObject(bool createIfMissing)
            => global::ProtoBuf.Extensible.GetExtensionObject(ref __pbn__extensionData, createIfMissing);

        [global::ProtoBuf.ProtoMember(1, Name = @"num_rows")]
        [global::System.ComponentModel.DefaultValue(0)]
        public int NumRows
        {
            get { return __pbn__NumRows ?? 0; }
            set { __pbn__NumRows = value; }
        }
        public bool ShouldSerializeNumRows() => __pbn__NumRows != null;
        public void ResetNumRows() => __pbn__NumRows = null;
        private int? __pbn__NumRows;

        [global::ProtoBuf.ProtoMember(2, Name = @"rows_sidecar")]
        public int RowsSidecar
        {
            get { return __pbn__RowsSidecar.GetValueOrDefault(); }
            set { __pbn__RowsSidecar = value; }
        }
        public bool ShouldSerializeRowsSidecar() => __pbn__RowsSidecar != null;
        public void ResetRowsSidecar() => __pbn__RowsSidecar = null;
        private int? __pbn__RowsSidecar;

        [global::ProtoBuf.ProtoMember(3, Name = @"indirect_data_sidecar")]
        public int IndirectDataSidecar
        {
            get { return __pbn__IndirectDataSidecar.GetValueOrDefault(); }
            set { __pbn__IndirectDataSidecar = value; }
        }
        public bool ShouldSerializeIndirectDataSidecar() => __pbn__IndirectDataSidecar != null;
        public void ResetIndirectDataSidecar() => __pbn__IndirectDataSidecar = null;
        private int? __pbn__IndirectDataSidecar;

    }

    [global::ProtoBuf.ProtoContract()]
    public partial class ColumnarRowBlockPB : global::ProtoBuf.IExtensible
    {
        private global::ProtoBuf.IExtension __pbn__extensionData;
        global::ProtoBuf.IExtension global::ProtoBuf.IExtensible.GetExtensionObject(bool createIfMissing)
            => global::ProtoBuf.Extensible.GetExtensionObject(ref __pbn__extensionData, createIfMissing);

        [global::ProtoBuf.ProtoMember(1, Name = @"columns")]
        public global::System.Collections.Generic.List<Column> Columns { get; } = new global::System.Collections.Generic.List<Column>();

        [global::ProtoBuf.ProtoMember(2, Name = @"num_rows")]
        public long NumRows
        {
            get { return __pbn__NumRows.GetValueOrDefault(); }
            set { __pbn__NumRows = value; }
        }
        public bool ShouldSerializeNumRows() => __pbn__NumRows != null;
        public void ResetNumRows() => __pbn__NumRows = null;
        private long? __pbn__NumRows;

        [global::ProtoBuf.ProtoContract()]
        public partial class Column : global::ProtoBuf.IExtensible
        {
            private global::ProtoBuf.IExtension __pbn__extensionData;
            global::ProtoBuf.IExtension global::ProtoBuf.IExtensible.GetExtensionObject(bool createIfMissing)
                => global::ProtoBuf.Extensible.GetExtensionObject(ref __pbn__extensionData, createIfMissing);

            [global::ProtoBuf.ProtoMember(1, Name = @"data_sidecar")]
            public int DataSidecar
            {
                get { return __pbn__DataSidecar.GetValueOrDefault(); }
                set { __pbn__DataSidecar = value; }
            }
            public bool ShouldSerializeDataSidecar() => __pbn__DataSidecar != null;
            public void ResetDataSidecar() => __pbn__DataSidecar = null;
            private int? __pbn__DataSidecar;

            [global::ProtoBuf.ProtoMember(2, Name = @"varlen_data_sidecar")]
            public int VarlenDataSidecar
            {
                get { return __pbn__VarlenDataSidecar.GetValueOrDefault(); }
                set { __pbn__VarlenDataSidecar = value; }
            }
            public bool ShouldSerializeVarlenDataSidecar() => __pbn__VarlenDataSidecar != null;
            public void ResetVarlenDataSidecar() => __pbn__VarlenDataSidecar = null;
            private int? __pbn__VarlenDataSidecar;

            [global::ProtoBuf.ProtoMember(3, Name = @"non_null_bitmap_sidecar")]
            public int NonNullBitmapSidecar
            {
                get { return __pbn__NonNullBitmapSidecar.GetValueOrDefault(); }
                set { __pbn__NonNullBitmapSidecar = value; }
            }
            public bool ShouldSerializeNonNullBitmapSidecar() => __pbn__NonNullBitmapSidecar != null;
            public void ResetNonNullBitmapSidecar() => __pbn__NonNullBitmapSidecar = null;
            private int? __pbn__NonNullBitmapSidecar;

        }

    }

    [global::ProtoBuf.ProtoContract()]
    public partial class RowOperationsPB : global::ProtoBuf.IExtensible
    {
        private global::ProtoBuf.IExtension __pbn__extensionData;
        global::ProtoBuf.IExtension global::ProtoBuf.IExtensible.GetExtensionObject(bool createIfMissing)
            => global::ProtoBuf.Extensible.GetExtensionObject(ref __pbn__extensionData, createIfMissing);

        [global::ProtoBuf.ProtoMember(2, Name = @"rows")]
        public byte[] Rows
        {
            get { return __pbn__Rows; }
            set { __pbn__Rows = value; }
        }
        public bool ShouldSerializeRows() => __pbn__Rows != null;
        public void ResetRows() => __pbn__Rows = null;
        private byte[] __pbn__Rows;

        [global::ProtoBuf.ProtoMember(3, Name = @"indirect_data")]
        public byte[] IndirectData
        {
            get { return __pbn__IndirectData; }
            set { __pbn__IndirectData = value; }
        }
        public bool ShouldSerializeIndirectData() => __pbn__IndirectData != null;
        public void ResetIndirectData() => __pbn__IndirectData = null;
        private byte[] __pbn__IndirectData;

        [global::ProtoBuf.ProtoContract()]
        public enum Type
        {
            [global::ProtoBuf.ProtoEnum(Name = @"UNKNOWN")]
            Unknown = 0,
            [global::ProtoBuf.ProtoEnum(Name = @"INSERT")]
            Insert = 1,
            [global::ProtoBuf.ProtoEnum(Name = @"UPDATE")]
            Update = 2,
            [global::ProtoBuf.ProtoEnum(Name = @"DELETE")]
            Delete = 3,
            [global::ProtoBuf.ProtoEnum(Name = @"UPSERT")]
            Upsert = 5,
            [global::ProtoBuf.ProtoEnum(Name = @"INSERT_IGNORE")]
            InsertIgnore = 10,
            [global::ProtoBuf.ProtoEnum(Name = @"SPLIT_ROW")]
            SplitRow = 4,
            [global::ProtoBuf.ProtoEnum(Name = @"RANGE_LOWER_BOUND")]
            RangeLowerBound = 6,
            [global::ProtoBuf.ProtoEnum(Name = @"RANGE_UPPER_BOUND")]
            RangeUpperBound = 7,
            [global::ProtoBuf.ProtoEnum(Name = @"EXCLUSIVE_RANGE_LOWER_BOUND")]
            ExclusiveRangeLowerBound = 8,
            [global::ProtoBuf.ProtoEnum(Name = @"INCLUSIVE_RANGE_UPPER_BOUND")]
            InclusiveRangeUpperBound = 9,
        }

    }

}

#pragma warning restore CS1591, CS0612, CS3021, IDE1006
