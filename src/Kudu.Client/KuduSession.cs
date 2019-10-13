using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace Kudu.Client
{
    public class KuduSession : IKuduSession
    {
        private readonly KuduClient _client;
        private readonly KuduSessionOptions _options;
        private readonly ChannelWriter<Operation> _writer;
        private readonly ChannelReader<Operation> _reader;
        private readonly SemaphoreSlim _singleFlush;

        private volatile CancellationTokenSource _flushCts;
        private volatile TaskCompletionSource<object> _flushTcs;

        private Task _consumeTask;

        public KuduSession(KuduClient client, KuduSessionOptions options)
        {
            _client = client;
            _options = options;

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
        }

        public async ValueTask DisposeAsync()
        {
            _writer.TryComplete();

            if (_consumeTask != null)
                await _consumeTask.ConfigureAwait(false);

            _singleFlush.Dispose();
            _flushCts.Dispose();
        }

        public void StartProcessing()
        {
            if (_consumeTask == null)
                _consumeTask = ConsumeAsync();
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
                await FillQueueAsync(queue).ConfigureAwait(false);

                if (queue.Count == 0)
                {
                    // It's possible to read 0 items when,
                    // 1) A flush was triggered when the queue was empty.
                    // 2) The session was disposed when the queue was empty.

                    ReleasePendingFlush();
                    continue;
                }

                await SendAsync(queue).ConfigureAwait(false);

                if (queue.Count < batchSize)
                {
                    ReleasePendingFlush();
                }

                queue.Clear();
            }
        }

        private async Task FillQueueAsync(List<Operation> queue)
        {
            ChannelReader<Operation> reader = _reader;
            int capacity = _options.BatchSize;

            while (queue.Count < capacity &&
                reader.TryRead(out var operation))
            {
                queue.Add(operation);
            }

            if (queue.Count == capacity)
                return;

            try
            {
                CancellationToken flushToken = _flushCts.Token;

                // Wait indefinitely for the first operation.
                Operation operation = await reader.ReadAsync(flushToken)
                    .ConfigureAwait(false);

                queue.Add(operation);

                using var timeout = new CancellationTokenSource(_options.FlushInterval);
                using var both = CancellationTokenSource.CreateLinkedTokenSource(
                    timeout.Token, flushToken);
                CancellationToken cancellationToken = both.Token;

                while (queue.Count < capacity)
                {
                    operation = await reader.ReadAsync(cancellationToken)
                        .ConfigureAwait(false);

                    queue.Add(operation);
                }
            }
            catch (OperationCanceledException) { }
            catch (ChannelClosedException) { }
            catch (Exception ex)
            {
                // TODO: Log warning.
                Console.WriteLine($"Unexpected exception reading session queue: {ex}");
            }
        }

        private void ReleasePendingFlush()
        {
            var flushCts = _flushCts;
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
            try
            {
                await _client.WriteRowAsync(queue, _options.ExternalConsistencyMode)
                    .ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                // TODO: Log warning, or infinite retry?
                Console.WriteLine($"Unexpected exception writing session data: {ex}");
            }
        }
    }
}
