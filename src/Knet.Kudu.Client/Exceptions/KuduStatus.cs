using Knet.Kudu.Client.Protobuf;
using Knet.Kudu.Client.Protobuf.Master;
using Knet.Kudu.Client.Protobuf.Transactions;
using Knet.Kudu.Client.Protobuf.Tserver;
using static Knet.Kudu.Client.Protobuf.AppStatusPB.Types;

namespace Knet.Kudu.Client.Exceptions;

/// <summary>
/// Representation of an error code and message.
/// </summary>
public class KuduStatus
{
    // Limit the message size we get from the servers as it can be quite large.
    internal const int MaxMessageLength = 32 * 1024;
    internal const string Abbreviation = "...";

    public ErrorCode Code { get; }

    public string Message { get; }

    /// <summary>
    /// Get the posix code associated with the error.
    /// -1 if no posix code is set.
    /// </summary>
    public int PosixCode { get; }

    private KuduStatus(ErrorCode code, string msg, int posixCode)
    {
        Code = code;
        PosixCode = posixCode;

        if (msg.Length > MaxMessageLength)
        {
            // Truncate the message and indicate that it was abbreviated.
            int messageLength = MaxMessageLength - Abbreviation.Length;
            Message = msg.Substring(0, messageLength) + Abbreviation;
        }
        else
        {
            Message = msg;
        }
    }

    private KuduStatus(AppStatusPB appStatusPB)
        : this(appStatusPB.Code, appStatusPB.Message, appStatusPB.PosixCode)
    {
    }

    private KuduStatus(ErrorCode code)
        : this(code, "", -1)
    {
    }

    /// <summary>
    /// Create a status object from a master error.
    /// </summary>
    /// <param name="masterErrorPB">Protobuf object received via RPC from the master.</param>
    internal static KuduStatus FromMasterErrorPB(MasterErrorPB masterErrorPB)
    {
        return new KuduStatus(masterErrorPB.Status);
    }

    /// <summary>
    /// Create a status object from a tablet server error.
    /// </summary>
    /// <param name="tserverErrorPB">Protobuf object received via RPC from the TS.</param>
    internal static KuduStatus FromTabletServerErrorPB(TabletServerErrorPB tserverErrorPB)
    {
        return new KuduStatus(tserverErrorPB.Status);
    }

    /// <summary>
    /// Create a status object from a TxnManager's error.
    /// </summary>
    /// <param name="pbError">Protobuf object received via RPC from the TxnManager.</param>
    /// <returns></returns>
    internal static KuduStatus FromTxnManagerErrorPB(TxnManagerErrorPB pbError)
    {
        return new KuduStatus(pbError.Status);
    }

    /// <summary>
    /// Create a Status object from a <see cref="AppStatusPB"/> protobuf object.
    /// </summary>
    /// <param name="pb">Protobuf object received via RPC from the server.</param>
    internal static KuduStatus FromPB(AppStatusPB pb)
    {
        return new KuduStatus(pb);
    }

    // Keep a single OK status object else we'll end up instantiating tons of them.
    public static KuduStatus Ok { get; } = new KuduStatus(ErrorCode.Ok);

    public static KuduStatus NotFound(string msg, int posixCode = -1) =>
        new(ErrorCode.NotFound, msg, posixCode);

    public static KuduStatus Corruption(string msg, int posixCode = -1) =>
        new(ErrorCode.Corruption, msg, posixCode);

    public static KuduStatus NotSupported(string msg, int posixCode = -1) =>
        new(ErrorCode.NotSupported, msg, posixCode);

    public static KuduStatus InvalidArgument(string msg, int posixCode = -1) =>
        new(ErrorCode.InvalidArgument, msg, posixCode);

    public static KuduStatus IOError(string msg, int posixCode = -1) =>
        new(ErrorCode.IoError, msg, posixCode);

    public static KuduStatus AlreadyPresent(string msg, int posixCode = -1) =>
        new(ErrorCode.AlreadyPresent, msg, posixCode);

    public static KuduStatus RuntimeError(string msg, int posixCode = -1) =>
        new(ErrorCode.RuntimeError, msg, posixCode);

    public static KuduStatus NetworkError(string msg, int posixCode = -1) =>
        new(ErrorCode.NetworkError, msg, posixCode);

    public static KuduStatus IllegalState(string msg, int posixCode = -1) =>
        new(ErrorCode.IllegalState, msg, posixCode);

    public static KuduStatus NotAuthorized(string msg, int posixCode = -1) =>
        new(ErrorCode.NotAuthorized, msg, posixCode);

    public static KuduStatus Aborted(string msg, int posixCode = -1) =>
        new(ErrorCode.Aborted, msg, posixCode);

    public static KuduStatus RemoteError(string msg, int posixCode = -1) =>
        new(ErrorCode.RemoteError, msg, posixCode);

    public static KuduStatus ServiceUnavailable(string msg, int posixCode = -1) =>
        new(ErrorCode.ServiceUnavailable, msg, posixCode);

    public static KuduStatus TimedOut(string msg, int posixCode = -1) =>
        new(ErrorCode.TimedOut, msg, posixCode);

    public static KuduStatus Uninitialized(string msg, int posixCode = -1) =>
        new(ErrorCode.Uninitialized, msg, posixCode);

    public static KuduStatus ConfigurationError(string msg, int posixCode = -1) =>
        new(ErrorCode.ConfigurationError, msg, posixCode);

    public static KuduStatus Incomplete(string msg, int posixCode = -1) =>
        new(ErrorCode.Incomplete, msg, posixCode);

    public static KuduStatus EndOfFile(string msg, int posixCode = -1) =>
        new(ErrorCode.EndOfFile, msg, posixCode);

    public static KuduStatus Cancelled(string msg, int posixCode = -1) =>
        new(ErrorCode.Cancelled, msg, posixCode);

    public bool IsOk => Code == ErrorCode.Ok;

    public bool IsCorruption => Code == ErrorCode.Corruption;

    public bool IsNotFound => Code == ErrorCode.NotFound;

    public bool IsNotSupported => Code == ErrorCode.NotSupported;

    public bool IsInvalidArgument => Code == ErrorCode.InvalidArgument;

    public bool IsIOError => Code == ErrorCode.IoError;

    public bool IsAlreadyPresent => Code == ErrorCode.AlreadyPresent;

    public bool IsRuntimeError => Code == ErrorCode.RuntimeError;

    public bool IsNetworkError => Code == ErrorCode.NetworkError;

    public bool IsIllegalState => Code == ErrorCode.IllegalState;

    public bool IsNotAuthorized => Code == ErrorCode.NotAuthorized;

    public bool IsAborted => Code == ErrorCode.Aborted;

    public bool IsRemoteError => Code == ErrorCode.RemoteError;

    public bool IsServiceUnavailable => Code == ErrorCode.ServiceUnavailable;

    public bool IsTimedOut => Code == ErrorCode.TimedOut;

    public bool IsUninitialized => Code == ErrorCode.Uninitialized;

    public bool IsConfigurationError => Code == ErrorCode.ConfigurationError;

    public bool IsIncomplete => Code == ErrorCode.Incomplete;

    public bool IsEndOfFile => Code == ErrorCode.EndOfFile;

    public bool IsCancelled => Code == ErrorCode.Cancelled;

    /// <summary>
    /// Return a human-readable version of the status code.
    /// </summary>
    private string GetCodeAsstring()
    {
        return Code switch
        {
            ErrorCode.Ok => "Ok",
            ErrorCode.NotFound => "Not found",
            ErrorCode.Corruption => "Corruption",
            ErrorCode.NotSupported => "Not implemented",
            ErrorCode.InvalidArgument => "Invalid argument",
            ErrorCode.IoError => "IO error",
            ErrorCode.AlreadyPresent => "Already present",
            ErrorCode.RuntimeError => "Runtime error",
            ErrorCode.NetworkError => "Network error",
            ErrorCode.IllegalState => "Illegal state",
            ErrorCode.NotAuthorized => "Not authorized",
            ErrorCode.Aborted => "Aborted",
            ErrorCode.RemoteError => "Remote error",
            ErrorCode.ServiceUnavailable => "Service unavailable",
            ErrorCode.TimedOut => "Timed out",
            ErrorCode.Uninitialized => "Uninitialized",
            ErrorCode.ConfigurationError => "Configuration error",
            ErrorCode.Incomplete => "Incomplete",
            ErrorCode.EndOfFile => "End of file",
            ErrorCode.Cancelled => "Cancelled",
            ErrorCode.UnknownError => "Unknown",
            _ => $"Unknown error ({Code})"
        };
    }

    public override string ToString()
    {
        string str = GetCodeAsstring();
        if (Code == ErrorCode.Ok)
        {
            return str;
        }
        str = $"{str}: {Message}";
        if (PosixCode != -1)
        {
            str = $"{str} (error {PosixCode})";
        }
        return str;
    }
}
