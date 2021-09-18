using System;
using System.Collections.Generic;
using System.Text;

namespace Knet.Kudu.Client.Exceptions;

/// <summary>
/// An exception that indicates the overall write operation succeeded,
/// but individual rows failed, such as inserting a row that already
/// exists, or updating or deleting a row that doesn't exist.
/// </summary>
public class KuduWriteException : KuduException
{
    public IReadOnlyList<KuduStatus> PerRowErrors { get; }

    public KuduWriteException(List<KuduStatus> errors)
        : base(GetStatus(errors))
    {
        PerRowErrors = errors;
    }

    private static KuduStatus GetStatus(List<KuduStatus> errors)
    {
        var stringBuilder = new StringBuilder("Per row errors:");
        foreach (var error in errors)
        {
            stringBuilder.Append($"{Environment.NewLine}{error.Message}");
        }
        return KuduStatus.InvalidArgument(stringBuilder.ToString());
    }
}
