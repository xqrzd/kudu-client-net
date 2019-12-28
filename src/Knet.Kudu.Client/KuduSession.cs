using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Knet.Kudu.Client.Logging;
using Microsoft.Extensions.Logging;

namespace Knet.Kudu.Client
{
    public class KuduSession : IKuduSession
    {
        private readonly ILogger _logger;
        private readonly KuduClient _client;
        private readonly KuduSessionOptions _options;
        private readonly ChannelWriter<Operation> _writer;
        private readonly ChannelReader<Operation> _reader;
        private readonly SemaphoreSlim _singleFlush;
        private readonly Task _consumeTask;

        private volatile CancellationTokenSource _flushCts;
        private volatile TaskCompletionSource<object> _flushTcs;

        public KuduSession(
            KuduClient client,
            KuduSessionOptions options,
            ILoggerFactory loggerFactory)
        {
            _client = client;
            _options = options;
            _logger = loggerFactory.CreateLogger<KuduSession>();

            var channelOptions = new BoundedChannelOptions(options.Capacity)
            {
                SingleWriter = options.SingleWriter,
                SingleReader = true,
                FullMode = BoundedChannelFullMode.Wait,
                AllowSynchronousContinuations = false
            };

            var channel = Channel.CreateBounded<Operation>(channelOptions);
            _writer = channel.Writer;
            _reader = channel.Reader;

            _singleFlush = new SemaphoreSlim(1, 1);
            _flushCts = new CancellationTokenSource();
            _flushTcs = new TaskCompletionSource<object>(
                TaskCreationOptions.RunContinuationsAsynchronously);

            _consumeTask = ConsumeAsync();
        }

        public async ValueTask DisposeAsync()
        {
            // TODO: Handle pending flushes.
            _writer.TryComplete();

            await _consumeTask.ConfigureAwait(false);

            _singleFlush.Dispose();
            _flushCts.Dispose();
        }

        public ValueTask EnqueueAsync(
            Operation operation,
            CancellationToken cancellationToken = default)
        {
            return _writer.WriteAsync(operation, cancellationToken);
        }

        public async Task FlushAsync(CancellationToken cancellationToken = default)
        {
            CancellationTokenRegistration registration = default;

            await _singleFlush.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                _flushCts.Cancel();

                var flushTcs = _flushTcs;

                if (cancellationToken.CanBeCanceled)
                {
                    registration = cancellationToken.Register(
                        s => ((TaskCompletionSource<object>)s).TrySetCanceled(),
                        state: flushTcs,
                        useSynchronizationContext: false);
                }

                await flushTcs.Task.ConfigureAwait(false);
                // TODO: After this we should have a new _flushCts for next time,
                // *except* if the passed cancellationToken was cancelled.
            }
            finally
            {
                registration.Dispose();

                _flushTcs = new TaskCompletionSource<object>(
                    TaskCreationOptions.RunContinuationsAsynchronously);

                _singleFlush.Release();
            }
        }

        private async Task ConsumeAsync()
        {
            var channelCompletionTask = _reader.Completion;
            int batchSize = _options.BatchSize;
            var queue = new List<Operation>(batchSize);

            while (!channelCompletionTask.IsCompleted)
            {
                bool flush = await DequeueAsync(queue).ConfigureAwait(false);

                if (queue.Count == 0)
                {
                    // It's possible to read 0 items when,
                    // 1) A flush was triggered when the queue was empty.
                    // 2) The session was disposed when the queue was empty.

                    if (flush)
                    {
                        CompletePendingFlush();
                    }

                    continue;
                }

                await SendAsync(queue).ConfigureAwait(false);

                if (flush)
                {
                    CompletePendingFlush();
                }

                queue.Clear();
            }
        }

        private async Task<bool> DequeueAsync(List<Operation> queue)
        {
            ChannelReader<Operation> reader = _reader;
            CancellationToken flushToken = _flushCts.Token;
            bool flushRequested = flushToken.IsCancellationRequested;
            int capacity = _options.BatchSize;

            // First try to quickly drain any existing items
            // in the queue, before we start waiting for new
            // items to be added.
            while (queue.Count < capacity &&
                reader.TryRead(out var operation))
            {
                queue.Add(operation);
            }

            if (queue.Count == capacity)
            {
                // We can't complete a pending flush here, because we
                // don't know if there are more items in the queue.
                return false;
            }

            try
            {
                if (queue.Count == 0)
                {
                    // Wait indefinitely for the first operation.
                    Operation operation = await reader.ReadAsync(flushToken)
                        .ConfigureAwait(false);

                    queue.Add(operation);
                }

                using var timeout = new CancellationTokenSource(_options.FlushInterval);
                using var both = CancellationTokenSource.CreateLinkedTokenSource(
                    timeout.Token, flushToken);
                CancellationToken cancellationToken = both.Token;

                while (queue.Count < capacity)
                {
                    Operation operation = await reader.ReadAsync(cancellationToken)
                        .ConfigureAwait(false);

                    queue.Add(operation);
                }
            }
            catch (OperationCanceledException) when (flushToken.IsCancellationRequested)
            {
                while (queue.Count < capacity &&
                    reader.TryRead(out var operation))
                {
                    queue.Add(operation);
                }
                flushRequested = true;
            }
            catch (ChannelClosedException) { }

            return flushRequested && queue.Count < capacity;
        }

        private void CompletePendingFlush()
        {
            var flushCts = _flushCts;
            Debug.Assert(flushCts.IsCancellationRequested);
            if (flushCts.IsCancellationRequested)
            {
                // Make a new CancellationToken before releasing the
                // flush task.
                flushCts.Dispose();
                _flushCts = new CancellationTokenSource();

                // Complete the flush.
                _flushTcs.TrySetResult(null);
            }
        }

        private async Task SendAsync(List<Operation> queue)
        {
            while (true)
            {
                try
                {
                    await _client.WriteRowAsync(queue, _options.ExternalConsistencyMode)
                        .ConfigureAwait(false);

                    return;
                }
                catch (Exception ex)
                {
                    _logger.ExceptionSendingSessionData(ex);
                    await Task.Delay(4000).ConfigureAwait(false);
                }
            }
        }
    }
}
