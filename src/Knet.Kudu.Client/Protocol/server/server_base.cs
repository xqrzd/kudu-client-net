// This file was generated by a tool; you should avoid making direct changes.
// Consider using 'partial classes' to extend these types
// Input: server_base.proto

#pragma warning disable CS1591, CS0612, CS3021, IDE1006
namespace Knet.Kudu.Client.Protocol.Server
{

    [global::ProtoBuf.ProtoContract()]
    public partial class ServerStatusPB : global::ProtoBuf.IExtensible
    {
        private global::ProtoBuf.IExtension __pbn__extensionData;
        global::ProtoBuf.IExtension global::ProtoBuf.IExtensible.GetExtensionObject(bool createIfMissing)
            => global::ProtoBuf.Extensible.GetExtensionObject(ref __pbn__extensionData, createIfMissing);

        [global::ProtoBuf.ProtoMember(1, Name = @"node_instance", IsRequired = true)]
        public global::Knet.Kudu.Client.Protocol.NodeInstancePB NodeInstance { get; set; }

        [global::ProtoBuf.ProtoMember(2, Name = @"bound_rpc_addresses")]
        public global::System.Collections.Generic.List<global::Knet.Kudu.Client.Protocol.HostPortPB> BoundRpcAddresses { get; } = new global::System.Collections.Generic.List<global::Knet.Kudu.Client.Protocol.HostPortPB>();

        [global::ProtoBuf.ProtoMember(3, Name = @"bound_http_addresses")]
        public global::System.Collections.Generic.List<global::Knet.Kudu.Client.Protocol.HostPortPB> BoundHttpAddresses { get; } = new global::System.Collections.Generic.List<global::Knet.Kudu.Client.Protocol.HostPortPB>();

        [global::ProtoBuf.ProtoMember(4, Name = @"version_info")]
        public global::Knet.Kudu.Client.Protocol.VersionInfoPB VersionInfo { get; set; }

    }

    [global::ProtoBuf.ProtoContract()]
    public partial class GetFlagsRequestPB : global::ProtoBuf.IExtensible
    {
        private global::ProtoBuf.IExtension __pbn__extensionData;
        global::ProtoBuf.IExtension global::ProtoBuf.IExtensible.GetExtensionObject(bool createIfMissing)
            => global::ProtoBuf.Extensible.GetExtensionObject(ref __pbn__extensionData, createIfMissing);

        [global::ProtoBuf.ProtoMember(1, Name = @"all_flags")]
        public bool AllFlags
        {
            get { return __pbn__AllFlags.GetValueOrDefault(); }
            set { __pbn__AllFlags = value; }
        }
        public bool ShouldSerializeAllFlags() => __pbn__AllFlags != null;
        public void ResetAllFlags() => __pbn__AllFlags = null;
        private bool? __pbn__AllFlags;

        [global::ProtoBuf.ProtoMember(2, Name = @"tags")]
        public global::System.Collections.Generic.List<string> Tags { get; } = new global::System.Collections.Generic.List<string>();

        [global::ProtoBuf.ProtoMember(3, Name = @"flags")]
        public global::System.Collections.Generic.List<string> Flags { get; } = new global::System.Collections.Generic.List<string>();

    }

    [global::ProtoBuf.ProtoContract()]
    public partial class GetFlagsResponsePB : global::ProtoBuf.IExtensible
    {
        private global::ProtoBuf.IExtension __pbn__extensionData;
        global::ProtoBuf.IExtension global::ProtoBuf.IExtensible.GetExtensionObject(bool createIfMissing)
            => global::ProtoBuf.Extensible.GetExtensionObject(ref __pbn__extensionData, createIfMissing);

        [global::ProtoBuf.ProtoMember(1, Name = @"flags")]
        public global::System.Collections.Generic.List<Flag> Flags { get; } = new global::System.Collections.Generic.List<Flag>();

        [global::ProtoBuf.ProtoContract()]
        public partial class Flag : global::ProtoBuf.IExtensible
        {
            private global::ProtoBuf.IExtension __pbn__extensionData;
            global::ProtoBuf.IExtension global::ProtoBuf.IExtensible.GetExtensionObject(bool createIfMissing)
                => global::ProtoBuf.Extensible.GetExtensionObject(ref __pbn__extensionData, createIfMissing);

            [global::ProtoBuf.ProtoMember(1, Name = @"name")]
            [global::System.ComponentModel.DefaultValue("")]
            public string Name
            {
                get { return __pbn__Name ?? ""; }
                set { __pbn__Name = value; }
            }
            public bool ShouldSerializeName() => __pbn__Name != null;
            public void ResetName() => __pbn__Name = null;
            private string __pbn__Name;

            [global::ProtoBuf.ProtoMember(2, Name = @"value")]
            [global::System.ComponentModel.DefaultValue("")]
            public string Value
            {
                get { return __pbn__Value ?? ""; }
                set { __pbn__Value = value; }
            }
            public bool ShouldSerializeValue() => __pbn__Value != null;
            public void ResetValue() => __pbn__Value = null;
            private string __pbn__Value;

            [global::ProtoBuf.ProtoMember(3, Name = @"tags")]
            public global::System.Collections.Generic.List<string> Tags { get; } = new global::System.Collections.Generic.List<string>();

            [global::ProtoBuf.ProtoMember(4, Name = @"is_default_value")]
            public bool IsDefaultValue
            {
                get { return __pbn__IsDefaultValue.GetValueOrDefault(); }
                set { __pbn__IsDefaultValue = value; }
            }
            public bool ShouldSerializeIsDefaultValue() => __pbn__IsDefaultValue != null;
            public void ResetIsDefaultValue() => __pbn__IsDefaultValue = null;
            private bool? __pbn__IsDefaultValue;

        }

    }

    [global::ProtoBuf.ProtoContract()]
    public partial class SetFlagRequestPB : global::ProtoBuf.IExtensible
    {
        private global::ProtoBuf.IExtension __pbn__extensionData;
        global::ProtoBuf.IExtension global::ProtoBuf.IExtensible.GetExtensionObject(bool createIfMissing)
            => global::ProtoBuf.Extensible.GetExtensionObject(ref __pbn__extensionData, createIfMissing);

        [global::ProtoBuf.ProtoMember(1, Name = @"flag", IsRequired = true)]
        public string Flag { get; set; }

        [global::ProtoBuf.ProtoMember(2, Name = @"value", IsRequired = true)]
        public string Value { get; set; }

        [global::ProtoBuf.ProtoMember(3, Name = @"force")]
        [global::System.ComponentModel.DefaultValue(false)]
        public bool Force
        {
            get { return __pbn__Force ?? false; }
            set { __pbn__Force = value; }
        }
        public bool ShouldSerializeForce() => __pbn__Force != null;
        public void ResetForce() => __pbn__Force = null;
        private bool? __pbn__Force;

    }

    [global::ProtoBuf.ProtoContract()]
    public partial class SetFlagResponsePB : global::ProtoBuf.IExtensible
    {
        private global::ProtoBuf.IExtension __pbn__extensionData;
        global::ProtoBuf.IExtension global::ProtoBuf.IExtensible.GetExtensionObject(bool createIfMissing)
            => global::ProtoBuf.Extensible.GetExtensionObject(ref __pbn__extensionData, createIfMissing);

        [global::ProtoBuf.ProtoMember(1, Name = @"result", IsRequired = true)]
        public Code Result { get; set; }

        [global::ProtoBuf.ProtoMember(2, Name = @"msg")]
        [global::System.ComponentModel.DefaultValue("")]
        public string Msg
        {
            get { return __pbn__Msg ?? ""; }
            set { __pbn__Msg = value; }
        }
        public bool ShouldSerializeMsg() => __pbn__Msg != null;
        public void ResetMsg() => __pbn__Msg = null;
        private string __pbn__Msg;

        [global::ProtoBuf.ProtoMember(3, Name = @"old_value")]
        [global::System.ComponentModel.DefaultValue("")]
        public string OldValue
        {
            get { return __pbn__OldValue ?? ""; }
            set { __pbn__OldValue = value; }
        }
        public bool ShouldSerializeOldValue() => __pbn__OldValue != null;
        public void ResetOldValue() => __pbn__OldValue = null;
        private string __pbn__OldValue;

        [global::ProtoBuf.ProtoContract()]
        public enum Code
        {
            [global::ProtoBuf.ProtoEnum(Name = @"UNKNOWN")]
            Unknown = 0,
            [global::ProtoBuf.ProtoEnum(Name = @"SUCCESS")]
            Success = 1,
            [global::ProtoBuf.ProtoEnum(Name = @"NO_SUCH_FLAG")]
            NoSuchFlag = 2,
            [global::ProtoBuf.ProtoEnum(Name = @"BAD_VALUE")]
            BadValue = 3,
            [global::ProtoBuf.ProtoEnum(Name = @"NOT_SAFE")]
            NotSafe = 4,
        }

    }

    [global::ProtoBuf.ProtoContract()]
    public partial class FlushCoverageRequestPB : global::ProtoBuf.IExtensible
    {
        private global::ProtoBuf.IExtension __pbn__extensionData;
        global::ProtoBuf.IExtension global::ProtoBuf.IExtensible.GetExtensionObject(bool createIfMissing)
            => global::ProtoBuf.Extensible.GetExtensionObject(ref __pbn__extensionData, createIfMissing);

    }

    [global::ProtoBuf.ProtoContract()]
    public partial class FlushCoverageResponsePB : global::ProtoBuf.IExtensible
    {
        private global::ProtoBuf.IExtension __pbn__extensionData;
        global::ProtoBuf.IExtension global::ProtoBuf.IExtensible.GetExtensionObject(bool createIfMissing)
            => global::ProtoBuf.Extensible.GetExtensionObject(ref __pbn__extensionData, createIfMissing);

        [global::ProtoBuf.ProtoMember(1, Name = @"success")]
        public bool Success
        {
            get { return __pbn__Success.GetValueOrDefault(); }
            set { __pbn__Success = value; }
        }
        public bool ShouldSerializeSuccess() => __pbn__Success != null;
        public void ResetSuccess() => __pbn__Success = null;
        private bool? __pbn__Success;

    }

    [global::ProtoBuf.ProtoContract()]
    public partial class CheckLeaksRequestPB : global::ProtoBuf.IExtensible
    {
        private global::ProtoBuf.IExtension __pbn__extensionData;
        global::ProtoBuf.IExtension global::ProtoBuf.IExtensible.GetExtensionObject(bool createIfMissing)
            => global::ProtoBuf.Extensible.GetExtensionObject(ref __pbn__extensionData, createIfMissing);

    }

    [global::ProtoBuf.ProtoContract()]
    public partial class CheckLeaksResponsePB : global::ProtoBuf.IExtensible
    {
        private global::ProtoBuf.IExtension __pbn__extensionData;
        global::ProtoBuf.IExtension global::ProtoBuf.IExtensible.GetExtensionObject(bool createIfMissing)
            => global::ProtoBuf.Extensible.GetExtensionObject(ref __pbn__extensionData, createIfMissing);

        [global::ProtoBuf.ProtoMember(1, Name = @"success")]
        public bool Success
        {
            get { return __pbn__Success.GetValueOrDefault(); }
            set { __pbn__Success = value; }
        }
        public bool ShouldSerializeSuccess() => __pbn__Success != null;
        public void ResetSuccess() => __pbn__Success = null;
        private bool? __pbn__Success;

        [global::ProtoBuf.ProtoMember(2, Name = @"found_leaks")]
        public bool FoundLeaks
        {
            get { return __pbn__FoundLeaks.GetValueOrDefault(); }
            set { __pbn__FoundLeaks = value; }
        }
        public bool ShouldSerializeFoundLeaks() => __pbn__FoundLeaks != null;
        public void ResetFoundLeaks() => __pbn__FoundLeaks = null;
        private bool? __pbn__FoundLeaks;

    }

    [global::ProtoBuf.ProtoContract()]
    public partial class ServerClockRequestPB : global::ProtoBuf.IExtensible
    {
        private global::ProtoBuf.IExtension __pbn__extensionData;
        global::ProtoBuf.IExtension global::ProtoBuf.IExtensible.GetExtensionObject(bool createIfMissing)
            => global::ProtoBuf.Extensible.GetExtensionObject(ref __pbn__extensionData, createIfMissing);

    }

    [global::ProtoBuf.ProtoContract()]
    public partial class ServerClockResponsePB : global::ProtoBuf.IExtensible
    {
        private global::ProtoBuf.IExtension __pbn__extensionData;
        global::ProtoBuf.IExtension global::ProtoBuf.IExtensible.GetExtensionObject(bool createIfMissing)
            => global::ProtoBuf.Extensible.GetExtensionObject(ref __pbn__extensionData, createIfMissing);

        [global::ProtoBuf.ProtoMember(1, Name = @"timestamp", DataFormat = global::ProtoBuf.DataFormat.FixedSize)]
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
    public partial class GetStatusRequestPB : global::ProtoBuf.IExtensible
    {
        private global::ProtoBuf.IExtension __pbn__extensionData;
        global::ProtoBuf.IExtension global::ProtoBuf.IExtensible.GetExtensionObject(bool createIfMissing)
            => global::ProtoBuf.Extensible.GetExtensionObject(ref __pbn__extensionData, createIfMissing);

    }

    [global::ProtoBuf.ProtoContract()]
    public partial class GetStatusResponsePB : global::ProtoBuf.IExtensible
    {
        private global::ProtoBuf.IExtension __pbn__extensionData;
        global::ProtoBuf.IExtension global::ProtoBuf.IExtensible.GetExtensionObject(bool createIfMissing)
            => global::ProtoBuf.Extensible.GetExtensionObject(ref __pbn__extensionData, createIfMissing);

        [global::ProtoBuf.ProtoMember(1, Name = @"status", IsRequired = true)]
        public ServerStatusPB Status { get; set; }

        [global::ProtoBuf.ProtoMember(2, Name = @"error")]
        public global::Knet.Kudu.Client.Protocol.AppStatusPB Error { get; set; }

    }

    [global::ProtoBuf.ProtoContract()]
    public partial class SetServerWallClockForTestsRequestPB : global::ProtoBuf.IExtensible
    {
        private global::ProtoBuf.IExtension __pbn__extensionData;
        global::ProtoBuf.IExtension global::ProtoBuf.IExtensible.GetExtensionObject(bool createIfMissing)
            => global::ProtoBuf.Extensible.GetExtensionObject(ref __pbn__extensionData, createIfMissing);

        [global::ProtoBuf.ProtoMember(1, Name = @"now_usec")]
        public ulong NowUsec
        {
            get { return __pbn__NowUsec.GetValueOrDefault(); }
            set { __pbn__NowUsec = value; }
        }
        public bool ShouldSerializeNowUsec() => __pbn__NowUsec != null;
        public void ResetNowUsec() => __pbn__NowUsec = null;
        private ulong? __pbn__NowUsec;

        [global::ProtoBuf.ProtoMember(2, Name = @"max_error_usec")]
        public ulong MaxErrorUsec
        {
            get { return __pbn__MaxErrorUsec.GetValueOrDefault(); }
            set { __pbn__MaxErrorUsec = value; }
        }
        public bool ShouldSerializeMaxErrorUsec() => __pbn__MaxErrorUsec != null;
        public void ResetMaxErrorUsec() => __pbn__MaxErrorUsec = null;
        private ulong? __pbn__MaxErrorUsec;

    }

    [global::ProtoBuf.ProtoContract()]
    public partial class SetServerWallClockForTestsResponsePB : global::ProtoBuf.IExtensible
    {
        private global::ProtoBuf.IExtension __pbn__extensionData;
        global::ProtoBuf.IExtension global::ProtoBuf.IExtensible.GetExtensionObject(bool createIfMissing)
            => global::ProtoBuf.Extensible.GetExtensionObject(ref __pbn__extensionData, createIfMissing);

        [global::ProtoBuf.ProtoMember(1, Name = @"success", IsRequired = true)]
        public bool Success { get; set; }

    }

    [global::ProtoBuf.ProtoContract()]
    public partial class DumpMemTrackersRequestPB : global::ProtoBuf.IExtensible
    {
        private global::ProtoBuf.IExtension __pbn__extensionData;
        global::ProtoBuf.IExtension global::ProtoBuf.IExtensible.GetExtensionObject(bool createIfMissing)
            => global::ProtoBuf.Extensible.GetExtensionObject(ref __pbn__extensionData, createIfMissing);

    }

    [global::ProtoBuf.ProtoContract()]
    public partial class DumpMemTrackersResponsePB : global::ProtoBuf.IExtensible
    {
        private global::ProtoBuf.IExtension __pbn__extensionData;
        global::ProtoBuf.IExtension global::ProtoBuf.IExtensible.GetExtensionObject(bool createIfMissing)
            => global::ProtoBuf.Extensible.GetExtensionObject(ref __pbn__extensionData, createIfMissing);

        [global::ProtoBuf.ProtoMember(1, Name = @"root_tracker")]
        public global::Knet.Kudu.Client.Protocol.MemTrackerPB RootTracker { get; set; }

    }

}

#pragma warning restore CS1591, CS0612, CS3021, IDE1006
