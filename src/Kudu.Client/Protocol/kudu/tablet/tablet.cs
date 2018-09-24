// This file was generated by a tool; you should avoid making direct changes.
// Consider using 'partial classes' to extend these types
// Input: tablet.proto

#pragma warning disable CS1591, CS0612, CS3021, IDE1006
namespace Kudu.Client.Protocol.Tablet
{

    [global::ProtoBuf.ProtoContract()]
    public partial class MemStoreTargetPB : global::ProtoBuf.IExtensible
    {
        private global::ProtoBuf.IExtension __pbn__extensionData;
        global::ProtoBuf.IExtension global::ProtoBuf.IExtensible.GetExtensionObject(bool createIfMissing)
            => global::ProtoBuf.Extensible.GetExtensionObject(ref __pbn__extensionData, createIfMissing);

        [global::ProtoBuf.ProtoMember(1, Name = @"mrs_id")]
        [global::System.ComponentModel.DefaultValue(-1)]
        public long MrsId
        {
            get { return __pbn__MrsId ?? -1; }
            set { __pbn__MrsId = value; }
        }
        public bool ShouldSerializeMrsId() => __pbn__MrsId != null;
        public void ResetMrsId() => __pbn__MrsId = null;
        private long? __pbn__MrsId;

        [global::ProtoBuf.ProtoMember(2, Name = @"rs_id")]
        [global::System.ComponentModel.DefaultValue(-1)]
        public long RsId
        {
            get { return __pbn__RsId ?? -1; }
            set { __pbn__RsId = value; }
        }
        public bool ShouldSerializeRsId() => __pbn__RsId != null;
        public void ResetRsId() => __pbn__RsId = null;
        private long? __pbn__RsId;

        [global::ProtoBuf.ProtoMember(3, Name = @"dms_id")]
        [global::System.ComponentModel.DefaultValue(-1)]
        public long DmsId
        {
            get { return __pbn__DmsId ?? -1; }
            set { __pbn__DmsId = value; }
        }
        public bool ShouldSerializeDmsId() => __pbn__DmsId != null;
        public void ResetDmsId() => __pbn__DmsId = null;
        private long? __pbn__DmsId;

    }

    [global::ProtoBuf.ProtoContract()]
    public partial class OperationResultPB : global::ProtoBuf.IExtensible
    {
        private global::ProtoBuf.IExtension __pbn__extensionData;
        global::ProtoBuf.IExtension global::ProtoBuf.IExtensible.GetExtensionObject(bool createIfMissing)
            => global::ProtoBuf.Extensible.GetExtensionObject(ref __pbn__extensionData, createIfMissing);

        [global::ProtoBuf.ProtoMember(1, Name = @"skip_on_replay")]
        [global::System.ComponentModel.DefaultValue(false)]
        public bool SkipOnReplay
        {
            get { return __pbn__SkipOnReplay ?? false; }
            set { __pbn__SkipOnReplay = value; }
        }
        public bool ShouldSerializeSkipOnReplay() => __pbn__SkipOnReplay != null;
        public void ResetSkipOnReplay() => __pbn__SkipOnReplay = null;
        private bool? __pbn__SkipOnReplay;

        [global::ProtoBuf.ProtoMember(2, Name = @"failed_status")]
        public global::Kudu.Client.Protocol.AppStatusPB FailedStatus { get; set; }

        [global::ProtoBuf.ProtoMember(3, Name = @"mutated_stores")]
        public global::System.Collections.Generic.List<MemStoreTargetPB> MutatedStores { get; } = new global::System.Collections.Generic.List<MemStoreTargetPB>();

    }

    [global::ProtoBuf.ProtoContract()]
    public partial class TxResultPB : global::ProtoBuf.IExtensible
    {
        private global::ProtoBuf.IExtension __pbn__extensionData;
        global::ProtoBuf.IExtension global::ProtoBuf.IExtensible.GetExtensionObject(bool createIfMissing)
            => global::ProtoBuf.Extensible.GetExtensionObject(ref __pbn__extensionData, createIfMissing);

        [global::ProtoBuf.ProtoMember(1, Name = @"ops")]
        public global::System.Collections.Generic.List<OperationResultPB> Ops { get; } = new global::System.Collections.Generic.List<OperationResultPB>();

    }

    [global::ProtoBuf.ProtoContract()]
    public partial class DeltaStatsPB : global::ProtoBuf.IExtensible
    {
        private global::ProtoBuf.IExtension __pbn__extensionData;
        global::ProtoBuf.IExtension global::ProtoBuf.IExtensible.GetExtensionObject(bool createIfMissing)
            => global::ProtoBuf.Extensible.GetExtensionObject(ref __pbn__extensionData, createIfMissing);

        [global::ProtoBuf.ProtoMember(1, Name = @"delete_count", IsRequired = true)]
        public long DeleteCount { get; set; }

        [global::ProtoBuf.ProtoMember(6, Name = @"reinsert_count")]
        [global::System.ComponentModel.DefaultValue(0)]
        public long ReinsertCount
        {
            get { return __pbn__ReinsertCount ?? 0; }
            set { __pbn__ReinsertCount = value; }
        }
        public bool ShouldSerializeReinsertCount() => __pbn__ReinsertCount != null;
        public void ResetReinsertCount() => __pbn__ReinsertCount = null;
        private long? __pbn__ReinsertCount;

        [global::ProtoBuf.ProtoMember(3, Name = @"min_timestamp", DataFormat = global::ProtoBuf.DataFormat.FixedSize, IsRequired = true)]
        public ulong MinTimestamp { get; set; }

        [global::ProtoBuf.ProtoMember(4, Name = @"max_timestamp", DataFormat = global::ProtoBuf.DataFormat.FixedSize, IsRequired = true)]
        public ulong MaxTimestamp { get; set; }

        [global::ProtoBuf.ProtoMember(5)]
        public global::System.Collections.Generic.List<ColumnStats> column_stats { get; } = new global::System.Collections.Generic.List<ColumnStats>();

        [global::ProtoBuf.ProtoContract()]
        public partial class ColumnStats : global::ProtoBuf.IExtensible
        {
            private global::ProtoBuf.IExtension __pbn__extensionData;
            global::ProtoBuf.IExtension global::ProtoBuf.IExtensible.GetExtensionObject(bool createIfMissing)
                => global::ProtoBuf.Extensible.GetExtensionObject(ref __pbn__extensionData, createIfMissing);

            [global::ProtoBuf.ProtoMember(1, Name = @"col_id", IsRequired = true)]
            public int ColId { get; set; }

            [global::ProtoBuf.ProtoMember(2, Name = @"update_count")]
            [global::System.ComponentModel.DefaultValue(0)]
            public long UpdateCount
            {
                get { return __pbn__UpdateCount ?? 0; }
                set { __pbn__UpdateCount = value; }
            }
            public bool ShouldSerializeUpdateCount() => __pbn__UpdateCount != null;
            public void ResetUpdateCount() => __pbn__UpdateCount = null;
            private long? __pbn__UpdateCount;

        }

    }

    [global::ProtoBuf.ProtoContract()]
    public partial class TabletStatusPB : global::ProtoBuf.IExtensible
    {
        private global::ProtoBuf.IExtension __pbn__extensionData;
        global::ProtoBuf.IExtension global::ProtoBuf.IExtensible.GetExtensionObject(bool createIfMissing)
            => global::ProtoBuf.Extensible.GetExtensionObject(ref __pbn__extensionData, createIfMissing);

        [global::ProtoBuf.ProtoMember(1, Name = @"tablet_id", IsRequired = true)]
        public string TabletId { get; set; }

        [global::ProtoBuf.ProtoMember(2, Name = @"table_name", IsRequired = true)]
        public string TableName { get; set; }

        [global::ProtoBuf.ProtoMember(3, Name = @"state")]
        [global::System.ComponentModel.DefaultValue(TabletStatePB.Unknown)]
        public TabletStatePB State
        {
            get { return __pbn__State ?? TabletStatePB.Unknown; }
            set { __pbn__State = value; }
        }
        public bool ShouldSerializeState() => __pbn__State != null;
        public void ResetState() => __pbn__State = null;
        private TabletStatePB? __pbn__State;

        [global::ProtoBuf.ProtoMember(8, Name = @"tablet_data_state")]
        [global::System.ComponentModel.DefaultValue(TabletDataState.TabletDataUnknown)]
        public TabletDataState TabletDataState
        {
            get { return __pbn__TabletDataState ?? TabletDataState.TabletDataUnknown; }
            set { __pbn__TabletDataState = value; }
        }
        public bool ShouldSerializeTabletDataState() => __pbn__TabletDataState != null;
        public void ResetTabletDataState() => __pbn__TabletDataState = null;
        private TabletDataState? __pbn__TabletDataState;

        [global::ProtoBuf.ProtoMember(4, Name = @"last_status", IsRequired = true)]
        public string LastStatus { get; set; }

        [global::ProtoBuf.ProtoMember(5, Name = @"start_key")]
        public byte[] StartKey
        {
            get { return __pbn__StartKey; }
            set { __pbn__StartKey = value; }
        }
        public bool ShouldSerializeStartKey() => __pbn__StartKey != null;
        public void ResetStartKey() => __pbn__StartKey = null;
        private byte[] __pbn__StartKey;

        [global::ProtoBuf.ProtoMember(6, Name = @"end_key")]
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

        [global::ProtoBuf.ProtoMember(7, Name = @"estimated_on_disk_size")]
        public long EstimatedOnDiskSize
        {
            get { return __pbn__EstimatedOnDiskSize.GetValueOrDefault(); }
            set { __pbn__EstimatedOnDiskSize = value; }
        }
        public bool ShouldSerializeEstimatedOnDiskSize() => __pbn__EstimatedOnDiskSize != null;
        public void ResetEstimatedOnDiskSize() => __pbn__EstimatedOnDiskSize = null;
        private long? __pbn__EstimatedOnDiskSize;

    }

}

#pragma warning restore CS1591, CS0612, CS3021, IDE1006
