using System;
using System.Collections.Generic;
using System.Text;
using Knet.Kudu.Client.Connection;

namespace Knet.Kudu.Client.Exceptions
{
    /// <summary>
    /// Indicates that the request failed because we couldn't find a leader.
    /// It is retried as long as the original call hasn't timed out.
    /// </summary>
    public class NoLeaderFoundException : RecoverableException
    {
        public NoLeaderFoundException(
            IReadOnlyList<HostAndPort> masterAddresses,
            Dictionary<HostAndPort, Exception> results)
            : base(
                  KuduStatus.NetworkError(GetMessage(masterAddresses, results)),
                  new AggregateException(results.Values))
        {
        }

        private static string GetMessage(
            IReadOnlyList<HostAndPort> masterAddresses,
            Dictionary<HostAndPort, Exception> results)
        {
            var sb = new StringBuilder("Unable to find master leader:");

            foreach (var address in masterAddresses)
            {
                sb.AppendLine();
                sb.Append($"\t{address} => ");

                if (results.TryGetValue(address, out var exception))
                    sb.Append(exception.Message);
                else
                    sb.Append("Not the leader");
            }

            return sb.ToString();
        }
    }
}
