using System;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;

namespace Knet.Kudu.Client.FunctionalTests.MiniCluster
{
    /// <summary>
    /// From CliWrap
    /// https://github.com/Tyrrrz/CliWrap/blob/master/CliWrap/Internal/ProcessEx.cs
    /// </summary>
    public class ProcessEx : IDisposable
    {
        private readonly Process _nativeProcess;
        private readonly TaskCompletionSource<object> _exitTcs = new TaskCompletionSource<object>(
            TaskCreationOptions.RunContinuationsAsynchronously);

        public int Id { get; private set; }

        public Stream StdIn { get; private set; }

        public Stream StdOut { get; private set; }

        public Stream StdErr { get; private set; }

        public int ExitCode { get; private set; }

        public DateTimeOffset StartTime { get; private set; }

        public DateTimeOffset ExitTime { get; private set; }

        public ProcessEx(ProcessStartInfo startInfo)
        {
            _nativeProcess = new Process { StartInfo = startInfo };
        }

        public void Start()
        {
            _nativeProcess.EnableRaisingEvents = true;
            _nativeProcess.Exited += (sender, args) =>
            {
                ExitTime = DateTimeOffset.Now;
                ExitCode = _nativeProcess.ExitCode;
                _exitTcs.TrySetResult(null);
            };

            _nativeProcess.Start();
            StartTime = DateTimeOffset.Now;

            Id = _nativeProcess.Id;
            StdIn = _nativeProcess.StandardInput.BaseStream;
            StdOut = _nativeProcess.StandardOutput.BaseStream;
            StdErr = _nativeProcess.StandardError.BaseStream;
        }

        public bool TryKill()
        {
            try
            {
                _nativeProcess.EnableRaisingEvents = false;
                _nativeProcess.Kill(true);

                return true;
            }
            catch
            {
                return false;
            }
            finally
            {
                _exitTcs.TrySetCanceled();
            }
        }

        public Task WaitUntilExitAsync() => _exitTcs.Task;

        public void Dispose()
        {
            // Kill the process if it's still alive by this point.
            if (!_nativeProcess.HasExited)
                TryKill();

            _nativeProcess.Dispose();
        }
    }
}
