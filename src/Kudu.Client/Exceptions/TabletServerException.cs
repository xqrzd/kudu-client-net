using System;
using Kudu.Client.Protocol.Tserver;

namespace Kudu.Client.Exceptions
{
    public class TabletServerException : Exception
    {
        public TabletServerErrorPB Error { get; }

        public TabletServerException(TabletServerErrorPB error)
            : base(error.Status.Message)
        {
            Error = error;
        }
    }
}
