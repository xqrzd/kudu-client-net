// This file was generated by a tool; you should avoid making direct changes.
// Consider using 'partial classes' to extend these types
// Input: tserver_admin.proto

#pragma warning disable CS1591, CS0612, CS3021, IDE1006
namespace Kudu.Client.Protocol.Tserver
{

    [global::ProtoBuf.ProtoContract()]
    public partial class AlterSchemaRequestPB : global::ProtoBuf.IExtensible
    {
        private global::ProtoBuf.IExtension __pbn__extensionData;
        global::ProtoBuf.IExtension global::ProtoBuf.IExtensible.GetExtensionObject(bool createIfMissing)
            => global::ProtoBuf.Extensible.GetExtensionObject(ref __pbn__extensionData, createIfMissing);

        [global::ProtoBuf.ProtoMember(5, Name = @"dest_uuid")]
        public byte[] DestUuid
        {
            get { return __pbn__DestUuid; }
            set { __pbn__DestUuid = value; }
        }
        public bool ShouldSerializeDestUuid() => __pbn__DestUuid != null;
        public void ResetDestUuid() => __pbn__DestUuid = null;
        private byte[] __pbn__DestUuid;

        [global::ProtoBuf.ProtoMember(1, Name = @"tablet_id", IsRequired = true)]
        public byte[] TabletId { get; set; }

        [global::ProtoBuf.ProtoMember(2, Name = @"schema", IsRequired = true)]
        public global::Kudu.Client.Protocol.SchemaPB Schema { get; set; }

        [global::ProtoBuf.ProtoMember(3, Name = @"schema_version", IsRequired = true)]
        public uint SchemaVersion { get; set; }

        [global::ProtoBuf.ProtoMember(4, Name = @"new_table_name")]
        [global::System.ComponentModel.DefaultValue("")]
        public string NewTableName
        {
            get { return __pbn__NewTableName ?? ""; }
            set { __pbn__NewTableName = value; }
        }
        public bool ShouldSerializeNewTableName() => __pbn__NewTableName != null;
        public void ResetNewTableName() => __pbn__NewTableName = null;
        private string __pbn__NewTableName;

    }

    [global::ProtoBuf.ProtoContract()]
    public partial class AlterSchemaResponsePB : global::ProtoBuf.IExtensible
    {
        private global::ProtoBuf.IExtension __pbn__extensionData;
        global::ProtoBuf.IExtension global::ProtoBuf.IExtensible.GetExtensionObject(bool createIfMissing)
            => global::ProtoBuf.Extensible.GetExtensionObject(ref __pbn__extensionData, createIfMissing);

        [global::ProtoBuf.ProtoMember(1, Name = @"error")]
        public TabletServerErrorPB Error { get; set; }

        [global::ProtoBuf.ProtoMember(2, Name = @"timestamp", DataFormat = global::ProtoBuf.DataFormat.FixedSize)]
        public ulong Timestamp
        {
            get { return __pbn__Timestamp.GetValueOrDefault(); }
            set { __pbn__Timestamp = value; }
        }
        public bool ShouldSerializeTimestamp() => __pbn__Timestamp != null;
        public void ResetTimestamp() => __pbn__Timestamp = null;
        private ulong? __pbn__Timestamp;

    }

    [global::ProtoBuf.ProtoContract()]
    public partial class CreateTabletRequestPB : global::ProtoBuf.IExtensible
    {
        private global::ProtoBuf.IExtension __pbn__extensionData;
        global::ProtoBuf.IExtension global::ProtoBuf.IExtensible.GetExtensionObject(bool createIfMissing)
            => global::ProtoBuf.Extensible.GetExtensionObject(ref __pbn__extensionData, createIfMissing);

        [global::ProtoBuf.ProtoMember(8, Name = @"dest_uuid")]
        public byte[] DestUuid
        {
            get { return __pbn__DestUuid; }
            set { __pbn__DestUuid = value; }
        }
        public bool ShouldSerializeDestUuid() => __pbn__DestUuid != null;
        public void ResetDestUuid() => __pbn__DestUuid = null;
        private byte[] __pbn__DestUuid;

        [global::ProtoBuf.ProtoMember(1, Name = @"table_id", IsRequired = true)]
        public byte[] TableId { get; set; }

        [global::ProtoBuf.ProtoMember(2, Name = @"tablet_id", IsRequired = true)]
        public byte[] TabletId { get; set; }

        [global::ProtoBuf.ProtoMember(3, Name = @"start_key")]
        public byte[] StartKey
        {
            get { return __pbn__StartKey; }
            set { __pbn__StartKey = value; }
        }
        public bool ShouldSerializeStartKey() => __pbn__StartKey != null;
        public void ResetStartKey() => __pbn__StartKey = null;
        private byte[] __pbn__StartKey;

        [global::ProtoBuf.ProtoMember(4, Name = @"end_key")]
        public byte[] EndKey
        {
            get { return __pbn__EndKey; }
            set { __pbn__EndKey = value; }
        }
        public bool ShouldSerializeEndKey() => __pbn__EndKey != null;
        public void ResetEndKey() => __pbn__EndKey = null;
        private byte[] __pbn__EndKey;

        [global::ProtoBuf.ProtoMember(9, Name = @"partition")]
        public global::Kudu.Client.Protocol.PartitionPB Partition { get; set; }

        [global::ProtoBuf.ProtoMember(5, Name = @"table_name", IsRequired = true)]
        public string TableName { get; set; }

        [global::ProtoBuf.ProtoMember(6, Name = @"schema", IsRequired = true)]
        public global::Kudu.Client.Protocol.SchemaPB Schema { get; set; }

        [global::ProtoBuf.ProtoMember(10, Name = @"partition_schema")]
        public global::Kudu.Client.Protocol.PartitionSchemaPB PartitionSchema { get; set; }

        [global::ProtoBuf.ProtoMember(7, Name = @"config", IsRequired = true)]
        public global::Kudu.Client.Protocol.Consensus.RaftConfigPB Config { get; set; }

    }

    [global::ProtoBuf.ProtoContract()]
    public partial class CreateTabletResponsePB : global::ProtoBuf.IExtensible
    {
        private global::ProtoBuf.IExtension __pbn__extensionData;
        global::ProtoBuf.IExtension global::ProtoBuf.IExtensible.GetExtensionObject(bool createIfMissing)
            => global::ProtoBuf.Extensible.GetExtensionObject(ref __pbn__extensionData, createIfMissing);

        [global::ProtoBuf.ProtoMember(1, Name = @"error")]
        public TabletServerErrorPB Error { get; set; }

    }

    [global::ProtoBuf.ProtoContract()]
    public partial class DeleteTabletRequestPB : global::ProtoBuf.IExtensible
    {
        private global::ProtoBuf.IExtension __pbn__extensionData;
        global::ProtoBuf.IExtension global::ProtoBuf.IExtensible.GetExtensionObject(bool createIfMissing)
            => global::ProtoBuf.Extensible.GetExtensionObject(ref __pbn__extensionData, createIfMissing);

        [global::ProtoBuf.ProtoMember(4, Name = @"dest_uuid")]
        public byte[] DestUuid
        {
            get { return __pbn__DestUuid; }
            set { __pbn__DestUuid = value; }
        }
        public bool ShouldSerializeDestUuid() => __pbn__DestUuid != null;
        public void ResetDestUuid() => __pbn__DestUuid = null;
        private byte[] __pbn__DestUuid;

        [global::ProtoBuf.ProtoMember(1, Name = @"tablet_id", IsRequired = true)]
        public byte[] TabletId { get; set; }

        [global::ProtoBuf.ProtoMember(2, Name = @"reason")]
        [global::System.ComponentModel.DefaultValue("")]
        public string Reason
        {
            get { return __pbn__Reason ?? ""; }
            set { __pbn__Reason = value; }
        }
        public bool ShouldSerializeReason() => __pbn__Reason != null;
        public void ResetReason() => __pbn__Reason = null;
        private string __pbn__Reason;

        [global::ProtoBuf.ProtoMember(3, Name = @"delete_type")]
        [global::System.ComponentModel.DefaultValue(global::Kudu.Client.Protocol.Tablet.TabletDataState.TabletDataTombstoned)]
        public global::Kudu.Client.Protocol.Tablet.TabletDataState DeleteType
        {
            get { return __pbn__DeleteType ?? global::Kudu.Client.Protocol.Tablet.TabletDataState.TabletDataTombstoned; }
            set { __pbn__DeleteType = value; }
        }
        public bool ShouldSerializeDeleteType() => __pbn__DeleteType != null;
        public void ResetDeleteType() => __pbn__DeleteType = null;
        private global::Kudu.Client.Protocol.Tablet.TabletDataState? __pbn__DeleteType;

        [global::ProtoBuf.ProtoMember(5, Name = @"cas_config_opid_index_less_or_equal")]
        public long CasConfigOpidIndexLessOrEqual
        {
            get { return __pbn__CasConfigOpidIndexLessOrEqual.GetValueOrDefault(); }
            set { __pbn__CasConfigOpidIndexLessOrEqual = value; }
        }
        public bool ShouldSerializeCasConfigOpidIndexLessOrEqual() => __pbn__CasConfigOpidIndexLessOrEqual != null;
        public void ResetCasConfigOpidIndexLessOrEqual() => __pbn__CasConfigOpidIndexLessOrEqual = null;
        private long? __pbn__CasConfigOpidIndexLessOrEqual;

    }

    [global::ProtoBuf.ProtoContract()]
    public partial class DeleteTabletResponsePB : global::ProtoBuf.IExtensible
    {
        private global::ProtoBuf.IExtension __pbn__extensionData;
        global::ProtoBuf.IExtension global::ProtoBuf.IExtensible.GetExtensionObject(bool createIfMissing)
            => global::ProtoBuf.Extensible.GetExtensionObject(ref __pbn__extensionData, createIfMissing);

        [global::ProtoBuf.ProtoMember(1, Name = @"error")]
        public TabletServerErrorPB Error { get; set; }

    }

    [global::ProtoBuf.ProtoContract()]
    public enum TSTabletManagerStatePB
    {
        [global::ProtoBuf.ProtoEnum(Name = @"UNKNOWN")]
        Unknown = 999,
        [global::ProtoBuf.ProtoEnum(Name = @"MANAGER_INITIALIZING")]
        ManagerInitializing = 0,
        [global::ProtoBuf.ProtoEnum(Name = @"MANAGER_RUNNING")]
        ManagerRunning = 1,
        [global::ProtoBuf.ProtoEnum(Name = @"MANAGER_QUIESCING")]
        ManagerQuiescing = 2,
        [global::ProtoBuf.ProtoEnum(Name = @"MANAGER_SHUTDOWN")]
        ManagerShutdown = 3,
    }

}

#pragma warning restore CS1591, CS0612, CS3021, IDE1006
