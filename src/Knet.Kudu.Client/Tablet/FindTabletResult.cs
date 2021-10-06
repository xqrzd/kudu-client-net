using System.Diagnostics.CodeAnalysis;

namespace Knet.Kudu.Client.Tablet;

internal readonly struct FindTabletResult
{
    public RemoteTablet? Tablet { get; }

    public int Index { get; }

    public byte[]? NonCoveredRangeStart { get; }

    public byte[]? NonCoveredRangeEnd { get; }

    public FindTabletResult(RemoteTablet tablet, int index)
    {
        Tablet = tablet;
        Index = index;
        NonCoveredRangeStart = null;
        NonCoveredRangeEnd = null;
    }

    public FindTabletResult(byte[] nonCoveredRangeStart, byte[] nonCoveredRangeEnd)
    {
        Tablet = null;
        Index = -1;
        NonCoveredRangeStart = nonCoveredRangeStart;
        NonCoveredRangeEnd = nonCoveredRangeEnd;
    }

    [MemberNotNullWhen(true, nameof(Tablet))]
    [MemberNotNullWhen(false, nameof(NonCoveredRangeStart), nameof(NonCoveredRangeEnd))]
    public bool IsCoveredRange => Tablet is not null;

    [MemberNotNullWhen(true, nameof(NonCoveredRangeStart), nameof(NonCoveredRangeEnd))]
    [MemberNotNullWhen(false, nameof(Tablet))]
    public bool IsNonCoveredRange => Tablet is null;
}
