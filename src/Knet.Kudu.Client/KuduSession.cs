using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Knet.Kudu.Client.Internal;
using Knet.Kudu.Client.Logging;
using Microsoft.Extensions.Logging;

namespace Knet.Kudu.Client;

/// <inheritdoc />
public sealed class KuduSession : IKuduSession
{
    private readonly ILogger _logger;
    private readonly KuduClient _client;
    private readonly KuduSessionOptions _options;
    private readonly ChannelWriter<KuduOperation> _writer;
    private readonly ChannelReader<KuduOperation> _reader;
    private readonly SemaphoreSlim _singleFlush;
    private readonly Task _consumeTask;
    private readonly long _txnId;

    private volatile CancellationTokenSource _flushCts;
    private volatile TaskCompletionSource _flushTcs;

    public KuduSession(
        KuduClient client,
        KuduSessionOptions options,
        ILoggerFactory loggerFactory,
        long txnId)
    {
        _client = client;
        _options = options;
        _txnId = txnId;
        _logger = loggerFactory.CreateLogger<KuduSession>();

        var channelOptions = new BoundedChannelOptions(options.Capacity)
        {
            SingleWriter = false,
            SingleReader = true,
            FullMode = BoundedChannelFullMode.Wait,
            AllowSynchronousContinuations = false
        };

        var channel = Channel.CreateBounded<KuduOperation>(channelOptions);
        _writer = channel.Writer;
        _reader = channel.Reader;

        _singleFlush = new SemaphoreSlim(1, 1);
        _flushCts = new CancellationTokenSource();
        _flushTcs = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);

        _consumeTask = ConsumeAsync();
    }

    public async ValueTask DisposeAsync()
    {
        _writer.TryComplete();

        await _consumeTask.ConfigureAwait(false);

        _singleFlush.Dispose();
        _flushCts.Dispose();
    }

    public ValueTask EnqueueAsync(
        KuduOperation operation,
        CancellationToken cancellationToken = default)
    {
        return _writer.WriteAsync(operation, cancellationToken);
    }

    public Task FlushAsync(CancellationToken cancellationToken = default)
    {
        return DoFlushAsync(cancellationToken).WaitAsync(cancellationToken);
    }

    private async Task DoFlushAsync(CancellationToken cancellationToken)
    {
        await _singleFlush.WaitAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            if (_reader.Completion.IsCompleted)
            {
                // This session is disposed.
                return;
            }

            try
            {
                _flushCts.Cancel();
                await _flushTcs.Task.ConfigureAwait(false);
                // After this we'll have a new _flushCts for next time.
            }
            finally
            {
                _flushTcs = new TaskCompletionSource(
                    TaskCreationOptions.RunContinuationsAsynchronously);
            }
        }
        finally
        {
            _singleFlush.Release();
        }
    }

    private async Task ConsumeAsync()
    {
        var channelCompletionTask = _reader.Completion;
        var batch = new List<KuduOperation>(_options.BatchSize);

        while (!channelCompletionTask.IsCompleted)
        {
            bool flush = await DequeueAsync(batch).ConfigureAwait(false);

            // It's possible to read 0 items when the queue is empty and
            // the session was flushed or disposed.
            if (batch.Count > 0)
            {
                await SendAsync(batch).ConfigureAwait(false);
            }

            if (flush)
            {
                CompletePendingFlush();
            }

            batch.Clear();
        }
    }

    private async Task<bool> DequeueAsync(List<KuduOperation> batch)
    {
        ChannelReader<KuduOperation> reader = _reader;
        CancellationToken flushToken = _flushCts.Token;
        bool flushRequested = flushToken.IsCancellationRequested;
        int capacity = _options.BatchSize;

        // Drain the queue before asynchronously waiting until the batch
        // is full or the flush interval is met.
        while (reader.TryRead(out var operation))
        {
            batch.Add(operation);

            if (batch.Count >= capacity)
            {
                // The batch is full, but we can't complete a pending flush
                // here as there could still be more items in the queue.
                return false;
            }
        }

        if (flushRequested)
        {
            // Short-circuit if a flush was requested and the queue is empty.
            return true;
        }

        try
        {
            if (batch.Count == 0)
            {
                // Wait indefinitely for the first operation.
                KuduOperation operation = await reader.ReadAsync(flushToken)
                    .ConfigureAwait(false);

                batch.Add(operation);
            }

            using var timeout = new CancellationTokenSource(_options.FlushInterval);
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(
                timeout.Token, flushToken);
            var token = linkedCts.Token;

            while (batch.Count < capacity)
            {
                KuduOperation operation = await reader.ReadAsync(token)
                    .ConfigureAwait(false);

                batch.Add(operation);
            }
        }
        catch (OperationCanceledException)
        {
            // We've hit the flush interval, or someone called FlushAsync().
            while (batch.Count < capacity &&
                reader.TryRead(out var operation))
            {
                batch.Add(operation);
            }
        }
        catch (ChannelClosedException)
        {
            // This session was disposed.
            // The queue is empty and is not accepting new items.
        }

        return flushRequested && batch.Count < capacity;
    }

    private void CompletePendingFlush()
    {
        // Make a new CancellationToken before releasing the flush task.
        _flushCts.Dispose();
        _flushCts = new CancellationTokenSource();

        // Complete the flush. FlushAsync() guarantees that _flushCts
        // won't be cancelled until we have a new _flushTcs.
        _flushTcs.TrySetResult();
    }

    private async Task SendAsync(List<KuduOperation> queue)
    {
        try
        {
            await _client.WriteAsync(queue, _options.ExternalConsistencyMode, _txnId)
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.ExceptionFlushingSessionData(ex, queue.Count, _txnId);

            var exceptionHandler = _options.ExceptionHandler;
            if (exceptionHandler is not null)
            {
                var queueCopy = new List<KuduOperation>(queue);
                var exceptionContext = new SessionExceptionContext(ex, queueCopy);

                try
                {
                    await exceptionHandler(exceptionContext).ConfigureAwait(false);
                }
                catch { }
            }
        }
    }
}
