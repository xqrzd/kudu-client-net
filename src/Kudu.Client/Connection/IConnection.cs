using System;
using System.IO.Pipelines;

namespace Kudu.Client.Connection
{
    public interface IConnection : IDuplexPipe, IDisposable
    {
    }
}
