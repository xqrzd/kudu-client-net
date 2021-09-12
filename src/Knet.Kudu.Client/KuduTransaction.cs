using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Knet.Kudu.Client.Exceptions;
using Knet.Kudu.Client.Internal;
using Knet.Kudu.Client.Protobuf.Transactions;
using Knet.Kudu.Client.Requests;
using Knet.Kudu.Client.Util;
using Microsoft.Extensions.Logging;

namespace Knet.Kudu.Client
{
    public sealed class KuduTransaction : IDisposable
    {
        private static readonly KuduTransactionSerializationOptions _defaultSerializationOptions = new();

        private readonly KuduClient _client;
        private readonly ILoggerFactory _loggerFactory;
        private readonly long _txnId;
        private readonly TimeSpan _keepaliveInterval;
        private readonly PeriodicTimer _keepaliveTimer;

        internal KuduTransaction(
            KuduClient client,
            ILoggerFactory loggerFactory,
            long txnId,
            TimeSpan keepaliveInterval)
        {
            if (txnId == KuduClient.InvalidTxnId)
                throw new ArgumentException("Invalid transaction id");

            _client = client;
            _loggerFactory = loggerFactory;
            _txnId = txnId;

            if (keepaliveInterval > TimeSpan.Zero)
            {
                _keepaliveInterval = keepaliveInterval;
                _keepaliveTimer = new PeriodicTimer(keepaliveInterval);
                _ = DoKeepaliveAsync();
            }
        }

        public void Dispose()
        {
            StopKeepaliveTimer();
        }

        public IKuduSession NewSession() => NewSession(KuduClient.DefaultSessionOptions);

        public IKuduSession NewSession(KuduSessionOptions options)
        {
            return new KuduSession(_client, options, _loggerFactory, _txnId);
        }

        /// <summary>
        /// Writes the given rows to Kudu without batching. For writing a large
        /// number of rows (>2000), consider using a session to handle batching.
        /// </summary>
        /// <param name="operations">The rows to write.</param>
        /// <param name="externalConsistencyMode">The external consistency mode for this write.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        public Task<WriteResponse> WriteAsync(
            IEnumerable<KuduOperation> operations,
            ExternalConsistencyMode externalConsistencyMode = ExternalConsistencyMode.ClientPropagated,
            CancellationToken cancellationToken = default)
        {
            return _client.WriteAsync(operations, externalConsistencyMode, _txnId, cancellationToken);
        }

        /// <summary>
        /// Commit the multi-row distributed transaction.
        /// </summary>
        /// <param name="cancellationToken">
        /// Used to cancel waiting for the commit. This will not rollback the transaction,
        /// use <see cref="RollbackAsync(CancellationToken)"/> for that.
        /// </param>
        public async Task CommitAsync(CancellationToken cancellationToken = default)
        {
            // TODO: Flush all sessions, check for any exceptions

            await CommitTransactionAsync(cancellationToken).ConfigureAwait(false);

            // Now, there is no need to continue sending keepalive messages: the
            // transaction should be in COMMIT_IN_PROGRESS state after successful
            // completion of the calls above, and the backend takes care of everything
            // else: nothing is required from the client side to successfully complete
            // the commit phase of the transaction past this point.
            StopKeepaliveTimer();
        }

        /// <summary>
        /// Rollback the multi-row distributed transaction. Once this method completes
        /// and no exception is thrown, a client has a guarantee that all write operations
        /// issued in the context of this transaction cannot be seen seen outside.
        /// </summary>
        /// <param name="cancellationToken">
        /// Used to cancel the rollback. Note that once the transaction has transitioned
        /// into ABORT_IN_PROGRESS, the rollback cannot be canceled.
        /// </param>
        public async Task RollbackAsync(CancellationToken cancellationToken = default)
        {
            await RollbackTransactionAsync(cancellationToken).ConfigureAwait(false);

            // There is no need to continue sending keepalive messages.
            StopKeepaliveTimer();
        }

        public async Task WaitForCommitAsync(CancellationToken cancellationToken = default)
        {
            var request = new GetTransactionStateRequestPB { TxnId = _txnId };
            var rpc = new GetTransactionStateRequest(request);

            while (true)
            {
                var response = await _client.SendRpcAsync(rpc, cancellationToken).ConfigureAwait(false);
                var state = response.State;

                if (response.HasCommitTimestamp)
                    _client.LastPropagatedTimestamp = (long)response.CommitTimestamp;

                if (IsTransactionCommited(state))
                    return;

                rpc.Attempt++;
            }
        }

        /// <summary>
        /// <para>
        /// Export information on the underlying transaction in a serialized form.
        /// </para>
        /// 
        /// <para>
        /// This method transforms this transaction into its serialized representation.
        /// </para>
        /// 
        /// <para>
        /// The serialized information on a Kudu transaction can be passed among
        /// different Kudu clients running at multiple nodes, so those separate
        /// Kudu clients can perform operations to be a part of the same distributed
        /// transaction. The resulting string is referred to as a "transaction token"
        /// and it can be deserialized into a <see cref="KuduTransaction"/> via the
        /// <see cref="KuduClient.NewTransactionFromToken(ReadOnlyMemory{byte})"/>.
        /// </para>
        /// </summary>
        public byte[] Serialize(KuduTransactionSerializationOptions options)
        {
            var tokenPb = new TxnTokenPB
            {
                TxnId = _txnId,
                EnableKeepalive = options.EnableKeepalive,
                KeepaliveMillis = (uint)_keepaliveInterval.TotalMilliseconds
            };

            return ProtobufHelper.ToByteArray(tokenPb);
        }

        /// <summary>
        /// <para>
        /// Export information on the underlying transaction in a serialized form.
        /// </para>
        /// 
        /// <para>
        /// This method transforms this transaction into its serialized representation.
        /// </para>
        /// 
        /// <para>
        /// The serialized information on a Kudu transaction can be passed among
        /// different Kudu clients running at multiple nodes, so those separate
        /// Kudu clients can perform operations to be a part of the same distributed
        /// transaction. The resulting string is referred to as a "transaction token"
        /// and it can be deserialized into a <see cref="KuduTransaction"/> via the
        /// <see cref="KuduClient.NewTransactionFromToken(ReadOnlyMemory{byte})"/>.
        /// </para>
        /// </summary>
        public byte[] Serialize() => Serialize(_defaultSerializationOptions);

        private async Task DoKeepaliveAsync()
        {
            var timer = _keepaliveTimer;

            while (await timer.WaitForNextTickAsync().ConfigureAwait(false))
            {
                try
                {
                    await KeepTransactionAliveAsync().ConfigureAwait(false);
                }
                catch
                {
                    // TODO: Log warning
                }
            }
        }

        private Task<CommitTransactionResponsePB> CommitTransactionAsync(CancellationToken cancellationToken)
        {
            var request = new CommitTransactionRequestPB { TxnId = _txnId };
            var rpc = new CommitTransactionRequest(request);
            return _client.SendRpcAsync(rpc, cancellationToken);
        }

        private Task RollbackTransactionAsync(CancellationToken cancellationToken)
        {
            var request = new AbortTransactionRequestPB { TxnId = _txnId };
            var rpc = new AbortTransactionRequest(request);
            return _client.SendRpcAsync(rpc, cancellationToken);
        }

        private Task KeepTransactionAliveAsync()
        {
            var request = new KeepTransactionAliveRequestPB { TxnId = _txnId };
            var rpc = new KeepTransactionAliveRequest(request);
            return _client.SendRpcAsync(rpc);
        }

        private void StopKeepaliveTimer()
        {
            _keepaliveTimer?.Dispose();
        }

        private static bool IsTransactionCommited(TxnStatePB txnState)
        {
            return txnState switch
            {
                TxnStatePB.Committed => true,
                TxnStatePB.CommitInProgress or TxnStatePB.FinalizeInProgress => false,

                TxnStatePB.AbortInProgress => throw new NonRecoverableException(
                    KuduStatus.Aborted("transaction is being aborted")),

                TxnStatePB.Aborted => throw new NonRecoverableException(
                    KuduStatus.Aborted("transaction was aborted")),

                TxnStatePB.Open => throw new NonRecoverableException(
                    KuduStatus.IllegalState("transaction is still open")),

                _ => throw new NonRecoverableException(
                    KuduStatus.NotSupported($"unexpected transaction state: {txnState}"))
            };
        }
    }
}
