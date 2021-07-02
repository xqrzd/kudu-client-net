using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Knet.Kudu.Client.FunctionalTests.MiniCluster
{
    /// <summary>
    /// From CliWrap
    /// https://github.com/Tyrrrz/CliWrap/blob/master/CliWrap/Utils/ProcessEx.cs
    /// </summary>
    public class ProcessEx : IDisposable
    {
        private readonly Process _nativeProcess;

        public Stream StdIn { get; private set; }

        public Stream StdOut { get; private set; }

        public Stream StdErr { get; private set; }

        public ProcessEx(ProcessStartInfo startInfo)
        {
            _nativeProcess = new Process { StartInfo = startInfo };
        }

        public void Start()
        {
            _nativeProcess.Start();

            StdIn = _nativeProcess.StandardInput.BaseStream;
            StdOut = _nativeProcess.StandardOutput.BaseStream;
            StdErr = _nativeProcess.StandardError.BaseStream;
        }

        public Task WaitForExitAsync(CancellationToken cancellationToken = default)
        {
            return _nativeProcess.WaitForExitAsync(cancellationToken);
        }

        public void Dispose()
        {
            _nativeProcess.Dispose();
        }
    }
}
