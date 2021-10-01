using System;
using System.Collections.Generic;
using System.Text;

namespace Knet.Kudu.Client.Exceptions;

/// <summary>
/// Indicates that the request failed because we couldn't find a leader.
/// It is retried as long as the original call hasn't timed out.
/// </summary>
public class NoLeaderFoundException : RecoverableException
{
    public NoLeaderFoundException(IEnumerable<string> results, Exception? innerException)
        : base(KuduStatus.NetworkError(GetMessage(results)), innerException)
    {
    }

    private static string GetMessage(IEnumerable<string> results)
    {
        var sb = new StringBuilder("Unable to find master leader:");

        foreach (var result in results)
        {
            sb.AppendLine();
            sb.Append($"\t{result}");
        }

        return sb.ToString();
    }
}
