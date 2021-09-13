#if !NET6_0_OR_GREATER

using System;
using System.Threading;
using System.Threading.Tasks;

namespace Knet.Kudu.Client.Internal
{
    internal sealed class PeriodicTimer : IDisposable
    {
        private readonly TimeSpan _period;
        private readonly CancellationTokenSource _stoppingCts;

        public PeriodicTimer(TimeSpan period)
        {
            _period = period;
            _stoppingCts = new CancellationTokenSource();
        }

        public void Dispose()
        {
            _stoppingCts.Cancel();
        }

        public async ValueTask<bool> WaitForNextTickAsync()
        {
            var token = _stoppingCts.Token;

            if (token.IsCancellationRequested)
            {
                return false;
            }

            try
            {
                await Task.Delay(_period, token).ConfigureAwait(false);
                return true;
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
                return false;
            }
        }
    }
}

#endif
