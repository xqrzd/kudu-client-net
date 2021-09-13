using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Knet.Kudu.Client.Exceptions;
using Knet.Kudu.Client.FunctionalTests.MiniCluster;
using Knet.Kudu.Client.FunctionalTests.Util;
using Knet.Kudu.Client.Protobuf.Transactions;
using Knet.Kudu.Client.Util;
using McMaster.Extensions.Xunit;
using Xunit;

namespace Knet.Kudu.Client.FunctionalTests
{
    [MiniKuduClusterTest]
    public class TransactionTests
    {
        /// <summary>
        /// Test scenario that starts a new transaction given an instance of
        /// KuduClient. The purpose of this test is to make sure it's possible
        /// to start a new transaction given a KuduClient object.
        /// </summary>
        [SkippableFact]
        public async Task TestNewTransaction()
        {
            await using var harness = await new MiniKuduClusterBuilder()
                .AddMasterServerFlag("--txn_manager_enabled")
                .AddTabletServerFlag("--enable_txn_system_client_init=true")
                .BuildHarnessAsync();

            await using var client = harness.CreateClient();

            using var transaction = await client.NewTransactionAsync();
            var buffer = transaction.Serialize();
            var transactionPb = TxnTokenPB.Parser.ParseFrom(buffer);
            Assert.True(transactionPb.HasTxnId);
            Assert.True(transactionPb.TxnId > KuduClient.InvalidTxnId);
            Assert.True(transactionPb.HasEnableKeepalive);
            // By default, keepalive is disabled for a serialized txn token.
            Assert.False(transactionPb.EnableKeepalive);
            Assert.True(transactionPb.HasKeepaliveMillis);
            Assert.True(transactionPb.KeepaliveMillis > 0);
        }

        /// <summary>
        /// Test scenario that starts many new transaction given an instance of
        /// KuduClient.
        /// </summary>
        [SkippableFact]
        public async Task TestStartManyTransactions()
        {
            await using var harness = await new MiniKuduClusterBuilder()
                .AddMasterServerFlag("--txn_manager_enabled")
                .AddTabletServerFlag("--enable_txn_system_client_init=true")
                .BuildHarnessAsync();

            await using var client = harness.CreateClient();
            var transactions = new List<KuduTransaction>();

            for (int i = 0; i < 100; i++)
            {
                var transaction = await client.NewTransactionAsync();
                transactions.Add(transaction);
            }

            foreach (var transaction in transactions)
            {
                await transaction.RollbackAsync();
            }
        }

        /// <summary>
        /// Test scenario that starts a new transaction and rolls it back.
        /// </summary>
        [SkippableFact]
        public async Task TestRollbackAnEmptyTransaction()
        {
            await using var harness = await new MiniKuduClusterBuilder()
                .AddMasterServerFlag("--txn_manager_enabled")
                .AddTabletServerFlag("--enable_txn_system_client_init=true")
                .BuildHarnessAsync();

            await using var client = harness.CreateClient();

            using var transaction = await client.NewTransactionAsync();
            await transaction.RollbackAsync();
            await transaction.RollbackAsync();

            // Try to rollback the same transaction using another handle that has been
            // constructed using serialize/deserialize sequence: it should be fine
            // since aborting a transaction has idempotent semantics for the back-end.
            var buffer = transaction.Serialize();
            var serdesTxn = client.NewTransactionFromToken(buffer);
            await serdesTxn.RollbackAsync();
        }

        /// <summary>
        /// Test scenario that starts a new transaction and commits it right away.
        /// </summary>
        [SkippableFact]
        public async Task TestCommitAnEmptyTransaction()
        {
            await using var harness = await new MiniKuduClusterBuilder()
                .AddMasterServerFlag("--txn_manager_enabled")
                .AddTabletServerFlag("--txn_schedule_background_tasks=false")
                .AddTabletServerFlag("--enable_txn_system_client_init=true")
                .BuildHarnessAsync();

            await using var client = harness.CreateClient();

            using var transaction = await client.NewTransactionAsync();
            await transaction.CommitAsync();
            await transaction.CommitAsync();

            // Try to commit the same transaction using another handle that has been
            // constructed using serialize/deserialize sequence: it should be fine
            // since committing a transaction has idempotent semantics for the back-end.
            var buffer = transaction.Serialize();
            var serdesTxn = client.NewTransactionFromToken(buffer);
            await serdesTxn.CommitAsync();
        }

        /// <summary>
        /// Test scenario that tries to commit a non-existent transaction.
        /// </summary>
        [SkippableFact]
        public async Task TestCommitNonExistentTransaction()
        {
            await using var harness = await new MiniKuduClusterBuilder()
                .AddMasterServerFlag("--txn_manager_enabled")
                .AddTabletServerFlag("--enable_txn_system_client_init=true")
                .BuildHarnessAsync();

            await using var client = harness.CreateClient();

            using var transaction = await client.NewTransactionAsync();
            using var fakeTransaction = MakeFakeTransaction(client, transaction);

            var exception = await Assert.ThrowsAsync<NonRecoverableException>(
                async () => await fakeTransaction.CommitAsync());

            Assert.True(exception.Status.IsInvalidArgument);
            Assert.Matches(".*transaction ID .* not found.*", exception.Message);
        }

        /// <summary>
        /// Transactional sessions can be closed as regular ones.
        /// </summary>
        [SkippableFact]
        public async Task TestTxnSessionClose()
        {
            await using var harness = await new MiniKuduClusterBuilder()
                .AddMasterServerFlag("--txn_manager_enabled")
                .AddTabletServerFlag("--enable_txn_system_client_init=true")
                .BuildHarnessAsync();

            await using var client = harness.CreateClient();

            var builder = ClientTestUtil.GetBasicSchema()
                .SetTableName(nameof(TestTxnSessionClose))
                .AddHashPartitions(2, "key");

            var table = await client.CreateTableAsync(builder);

            // Open and close an empty transaction session.
            {
                using var transaction = await client.NewTransactionAsync();
                await using var session = transaction.NewSession();
            }

            // Open new transaction, insert one row for a session, close the session
            // and then rollback the transaction. No rows should be persisted.
            {
                using var transaction = await client.NewTransactionAsync();
                await using var session = transaction.NewSession();

                var insert = ClientTestUtil.CreateBasicSchemaInsert(table, 1);
                await session.EnqueueAsync(insert);
                await session.FlushAsync();

                await transaction.RollbackAsync();

                var scanner = client.NewScanBuilder(table)
                    .SetReadMode(ReadMode.ReadYourWrites)
                    .Build();

                Assert.Equal(0, await ClientTestUtil.CountRowsInScanAsync(scanner));
            }
        }

        /// <summary>
        /// Verify how KuduTransaction.WaitForCommitAsync() works for a transaction
        /// in a few special cases.
        /// </summary>
        [SkippableFact]
        public async Task TestIsCommitCompleteSpecialCases()
        {
            await using var harness = await new MiniKuduClusterBuilder()
                .AddMasterServerFlag("--txn_manager_enabled")
                .AddTabletServerFlag("--txn_schedule_background_tasks=false")
                .AddTabletServerFlag("--enable_txn_system_client_init=true")
                .BuildHarnessAsync();

            await using var client = harness.CreateClient();
            using var transaction = await client.NewTransactionAsync();

            var exception1 = await Assert.ThrowsAsync<NonRecoverableException>(
                async () => await transaction.WaitForCommitAsync());

            Assert.True(exception1.Status.IsIllegalState);
            Assert.Equal("transaction is still open", exception1.Message);

            await transaction.RollbackAsync();

            var exception2 = await Assert.ThrowsAsync<NonRecoverableException>(
                async () => await transaction.WaitForCommitAsync());

            Assert.True(exception2.Status.IsAborted);
            Assert.Equal("transaction is being aborted", exception2.Message);

            using var fakeTransaction = MakeFakeTransaction(client, transaction);
            var exception3 = await Assert.ThrowsAsync<NonRecoverableException>(
                async () => await fakeTransaction.WaitForCommitAsync());

            Assert.True(exception3.Status.IsInvalidArgument);
            Assert.Matches(".*transaction ID .* not found.*", exception3.Message);
        }

        /// <summary>
        /// Test scenario that starts a new empty transaction, commits it, and waits
        /// for the transaction to be committed.
        /// </summary>
        [SkippableFact]
        public async Task TestCommitAnEmptyTransactionWait()
        {
            await using var harness = await new MiniKuduClusterBuilder()
                .AddMasterServerFlag("--txn_manager_enabled")
                .AddTabletServerFlag("--enable_txn_system_client_init=true")
                .BuildHarnessAsync();

            await using var client = harness.CreateClient();

            using var transaction = await client.NewTransactionAsync();
            await transaction.CommitAsync();
            await transaction.WaitForCommitAsync();
        }

        /// <summary>
        /// Test scenario that tries to rollback a non-existent transaction.
        /// </summary>
        [SkippableFact]
        public async Task TestRollbackNonExistentTransaction()
        {
            await using var harness = await new MiniKuduClusterBuilder()
                .AddMasterServerFlag("--txn_manager_enabled")
                .AddTabletServerFlag("--enable_txn_system_client_init=true")
                .BuildHarnessAsync();

            await using var client = harness.CreateClient();

            using var transaction = await client.NewTransactionAsync();
            using var fakeTransaction = MakeFakeTransaction(client, transaction);

            var exception = await Assert.ThrowsAsync<NonRecoverableException>(
                async () => await fakeTransaction.RollbackAsync());

            Assert.True(exception.Status.IsInvalidArgument);
            Assert.Matches(".*transaction ID .* not found.*", exception.Message);
        }

        /// <summary>
        /// Try to start a transaction when the backend doesn't have the required
        /// functionality (e.g. a backend which predates the introduction of the
        /// txn-related functionality).
        /// </summary>
        [SkippableFact]
        public async Task TestTxnOpsWithoutTxnManager()
        {
            await using var harness = await new MiniKuduClusterBuilder()
                .AddMasterServerFlag("--txn_manager_enabled=false")
                .AddTabletServerFlag("--enable_txn_system_client_init=true")
                .BuildHarnessAsync();

            await using var client = harness.CreateClient();

            var exception = await Assert.ThrowsAsync<RpcRemoteException>(
                async () => await client.NewTransactionAsync());

            Assert.True(exception.Status.IsRemoteError);
            Assert.Matches(".* Not found: .*", exception.Message);
            Assert.Matches(
                ".* kudu.transactions.TxnManagerService not registered on Master",
                exception.Message);
        }

        /// <summary>
        /// Verify that a transaction token created by the KuduTransaction.Serialize()
        /// functionality (e.g. a backend which predates the introduction of the
        /// txn-related functionality).
        /// </summary>
        [SkippableFact]
        public async Task TestSerializationOptions()
        {
            await using var harness = await new MiniKuduClusterBuilder()
                .AddMasterServerFlag("--txn_manager_enabled")
                .AddTabletServerFlag("--enable_txn_system_client_init=true")
                .BuildHarnessAsync();

            await using var client = harness.CreateClient();
            using var transaction = await client.NewTransactionAsync();

            // Check the keepalive settings when serializing/deserializing with default
            // settings for SerializationOptions.
            {
                var buffer = transaction.Serialize();
                var pb = TxnTokenPB.Parser.ParseFrom(buffer);
                Assert.True(pb.HasKeepaliveMillis);
                Assert.True(pb.KeepaliveMillis > 0);
                Assert.True(pb.HasEnableKeepalive);
                Assert.False(pb.EnableKeepalive);
            }

            // Same as above, but supply an instance of SerializationOptions with
            // default settings created by the constructor.
            {
                var options = new KuduTransactionSerializationOptions();
                var buffer = transaction.Serialize(options);
                var pb = TxnTokenPB.Parser.ParseFrom(buffer);
                Assert.True(pb.HasKeepaliveMillis);
                Assert.True(pb.KeepaliveMillis > 0);
                Assert.True(pb.HasEnableKeepalive);
                Assert.False(pb.EnableKeepalive);
            }

            // Same as above, but explicitly disable keepalive for an instance of
            // SerializationOptions.
            {
                var options = new KuduTransactionSerializationOptions
                {
                    EnableKeepalive = false
                };
                var buffer = transaction.Serialize(options);
                var pb = TxnTokenPB.Parser.ParseFrom(buffer);
                Assert.True(pb.HasKeepaliveMillis);
                Assert.True(pb.KeepaliveMillis > 0);
                Assert.True(pb.HasEnableKeepalive);
                Assert.False(pb.EnableKeepalive);
            }

            // Explicitly enable keepalive with SerializationOptions.
            {
                var options = new KuduTransactionSerializationOptions
                {
                    EnableKeepalive = true
                };
                var buffer = transaction.Serialize(options);
                var pb = TxnTokenPB.Parser.ParseFrom(buffer);
                Assert.True(pb.HasKeepaliveMillis);
                Assert.True(pb.KeepaliveMillis > 0);
                Assert.True(pb.HasEnableKeepalive);
                Assert.True(pb.EnableKeepalive);
            }
        }

        /// <summary>
        /// Test that a KuduTransaction created by KuduClient.NewTransactionAsync()
        /// automatically sends keepalive messages.
        /// </summary>
        [SkippableFact]
        public async Task TestKeepaliveBasic()
        {
            await using var harness = await new MiniKuduClusterBuilder()
                .AddMasterServerFlag("--txn_manager_enabled")
                .AddTabletServerFlag("--txn_keepalive_interval_ms=200")
                .AddTabletServerFlag("--txn_staleness_tracker_interval_ms=50")
                .AddTabletServerFlag("--enable_txn_system_client_init=true")
                .BuildHarnessAsync();

            await using var client = harness.CreateClient();

            using (var transaction = await client.NewTransactionAsync())
            {
                var buffer = transaction.Serialize();
                var transactionPb = TxnTokenPB.Parser.ParseFrom(buffer);
                Assert.True(transactionPb.HasKeepaliveMillis);
                var keepaliveMillis = (int)transactionPb.KeepaliveMillis;
                Assert.True(keepaliveMillis > 0);
                await Task.Delay(3 * keepaliveMillis);
                // It should be possible to commit the transaction since it supposed to be
                // open at this point even after multiples of the inactivity timeout
                // interval.
                await transaction.CommitAsync();
            }

            using (var transaction = await client.NewTransactionAsync())
            {
                var buffer = transaction.Serialize();
                var transactionPb = TxnTokenPB.Parser.ParseFrom(buffer);
                Assert.True(transactionPb.HasKeepaliveMillis);
                var keepaliveMillis = (int)transactionPb.KeepaliveMillis;
                Assert.True(keepaliveMillis > 0);

                transaction.Dispose();

                // Keep the handle around without any activity for longer than the
                // keepalive timeout interval.
                await Task.Delay(3 * keepaliveMillis);

                // At this point, the underlying transaction should be automatically
                // aborted by the backend. An attempt to commit the transaction should
                // fail because the transaction is assumed to be already aborted at this
                // point.
                var exception = await Assert.ThrowsAsync<NonRecoverableException>(
                    async () => await transaction.CommitAsync());

                Assert.Matches(".* transaction ID .* is not open: state: ABORT.*",
                    exception.Message);

                // Verify that KuduTransaction.RollbackAsync() successfully runs on a
                // transaction handle if the underlying transaction is already aborted
                // automatically by the backend. Rolling back the transaction explicitly
                // should succeed since it's a pure no-op: rolling back a transaction has
                // idempotent semantics.
                await transaction.RollbackAsync();
            }
        }

        /// <summary>
        /// Test that a KuduTransaction created by KuduClient.NewTransactionFromToken()
        /// automatically sends or doesn't send keepalive heartbeat messages
        /// depending on the serialization options used while serializing the handle
        /// into a transaction token.
        /// </summary>
        [SkippableFact]
        public async Task TestKeepaliveForDeserializedHandle()
        {
            await using var harness = await new MiniKuduClusterBuilder()
                .AddMasterServerFlag("--txn_manager_enabled")
                .AddTabletServerFlag("--txn_keepalive_interval_ms=200")
                .AddTabletServerFlag("--txn_schedule_background_tasks=false")
                .AddTabletServerFlag("--txn_staleness_tracker_interval_ms=50")
                .AddTabletServerFlag("--enable_txn_system_client_init=true")
                .BuildHarnessAsync();

            await using var client = harness.CreateClient();

            // Check the keepalive behavior when serializing/deserializing with default
            // settings for KuduTransactionSerializationOptions.
            using (var transaction = await client.NewTransactionAsync())
            {
                var buffer = transaction.Serialize();
                var txnPb = TxnTokenPB.Parser.ParseFrom(buffer);
                Assert.True(txnPb.HasKeepaliveMillis);
                var keepaliveMillis = (int)txnPb.KeepaliveMillis;
                Assert.True(keepaliveMillis > 0);

                var serdesTxn = client.NewTransactionFromToken(buffer);

                // Stop sending automatic keepalive messages from 'transaction' handle.
                transaction.Dispose();

                // Keep the handle around without any activity for longer than the
                // keepalive timeout interval.
                await Task.Delay(3 * keepaliveMillis);

                // At this point, the underlying transaction should be automatically
                // aborted by the backend: the 'transaction' handle should not send any
                // heartbeats anymore since it's closed, and the 'serdesTxn' handle
                // should not be sending any heartbeats.
                var exception = await Assert.ThrowsAsync<NonRecoverableException>(
                    async () => await serdesTxn.CommitAsync());

                Assert.Matches(".* transaction ID .* is not open: state: ABORT.*",
                    exception.Message);

                // Verify that KuduTransaction.RollbackAsync() successfully runs on both
                // transaction handles if the underlying transaction is already aborted
                // automatically by the backend.
                await transaction.RollbackAsync();
                await serdesTxn.RollbackAsync();
            }
        }

        /// <summary>
        /// An empty transaction doesn't have a timestamp, so there is nothing to
        /// propagate back to client when an empty transaction is committed, so the
        /// timestamp propagated to the client side should stay unchanged.
        /// </summary>
        [SkippableFact]
        public async Task TestPropagateEmptyTxnCommitTimestamp()
        {
            await using var harness = await new MiniKuduClusterBuilder()
                .AddMasterServerFlag("--txn_manager_enabled")
                // Inject latency to have a chance spotting the transaction in the
                // FINALIZE_IN_PROGRESS state and make KuduTransaction.WaitForCommitAsync()
                // have to poll multiple times.
                .AddTabletServerFlag("--txn_status_manager_inject_latency_finalize_commit_ms=250")
                .AddTabletServerFlag("--enable_txn_system_client_init=true")
                .BuildHarnessAsync();

            await using var client = harness.CreateClient();
            using var transaction = await client.NewTransactionAsync();

            var tsBeforeCommit = client.LastPropagatedTimestamp;
            await transaction.CommitAsync();
            await transaction.WaitForCommitAsync();

            // Just in case, linger a bit after commit has been finalized, checking
            // for the timestamp propagated to the client side.
            for (int i = 0; i < 10; i++)
            {
                await Task.Delay(10);
                Assert.Equal(tsBeforeCommit, client.LastPropagatedTimestamp);
            }
        }

        /// <summary>
        /// This scenario validates the propagation of the commit timestamp for a
        /// multi-row transaction when committing the transaction via CommitAsync()
        /// and waiting for completion with WaitForCommitAsync().
        /// </summary>
        [SkippableFact]
        public async Task TestPropagateTxnCommitTimestamp()
        {
            await using var harness = await new MiniKuduClusterBuilder()
                .AddMasterServerFlag("--txn_manager_enabled")
                // Inject latency to have a chance spotting the transaction in the
                // FINALIZE_IN_PROGRESS state and make KuduTransaction.WaitForCommitAsync()
                // have to poll multiple times.
                .AddTabletServerFlag("--txn_status_manager_inject_latency_finalize_commit_ms=250")
                .AddTabletServerFlag("--enable_txn_system_client_init=true")
                .BuildHarnessAsync();

            await using var client = harness.CreateClient();

            var builder = ClientTestUtil.GetBasicSchema()
                .SetTableName(nameof(TestPropagateTxnCommitTimestamp))
                .AddHashPartitions(8, "key");

            var table = await client.CreateTableAsync(builder);

            // Make sure the commit timestamp for a transaction is propagated to the
            // client upon committing a transaction.
            using (var transaction = await client.NewTransactionAsync())
            {
                // Insert many rows: the goal is to get at least one row inserted into
                // every tablet of the hash-partitioned test table, so every tablet would
                // be a participant in the transaction, and most likely every tablet
                // server would be involved.
                var inserts = Enumerable
                    .Range(0, 128)
                    .Select(key => ClientTestUtil.CreateBasicSchemaInsert(table, key));

                await transaction.WriteAsync(inserts);
                await CommitAndVerifyTransactionAsync(client, transaction);
            }

            // Make sure the commit timestamp for a transaction is propagated to the
            // client upon committing a transaction (using a session).
            using (var transaction = await client.NewTransactionAsync())
            {
                await using var session = transaction.NewSession();

                // Insert many rows: the goal is to get at least one row inserted into
                // every tablet of the hash-partitioned test table, so every tablet would
                // be a participant in the transaction, and most likely every tablet
                // server would be involved.
                for (int key = 128; key < 256; key++)
                {
                    var insert = ClientTestUtil.CreateBasicSchemaInsert(table, key);
                    await session.EnqueueAsync(insert);
                }

                await session.FlushAsync();
                await CommitAndVerifyTransactionAsync(client, transaction);
            }
        }

        /// <summary>
        /// Test to verify that Kudu client is able to switch to TxnManager hosted by
        /// other kudu-master process when the previously used one isn't available.
        /// </summary>
        [SkippableFact]
        public async Task TestSwitchToOtherTxnManager()
        {
            await using var harness = await new MiniKuduClusterBuilder()
                .AddMasterServerFlag("--txn_manager_enabled")
                // Set Raft heartbeat interval short for faster test runtime: speed up
                // leader failure detection and new leader election.
                .AddMasterServerFlag("--raft_heartbeat_interval_ms=100")
                .AddTabletServerFlag("--enable_txn_system_client_init=true")
                .BuildHarnessAsync();

            await using var client = harness.CreateClient();

            var builder = ClientTestUtil.GetBasicSchema()
                .SetTableName(nameof(TestSwitchToOtherTxnManager))
                .AddHashPartitions(2, "key");

            var table = await client.CreateTableAsync(builder);

            // Start a transaction, then restart every available TxnManager instance
            // before attempting any txn-related operation.
            using (var transaction = await client.NewTransactionAsync())
            {
                var insert = ClientTestUtil.CreateBasicSchemaInsert(table, 0);
                await transaction.WriteAsync(new[] { insert });

                await harness.KillAllMasterServersAsync();
                await harness.StartAllMasterServersAsync();

                // Querying the status of a transaction should be possible, as usual.
                // Since the transaction is still open, KuduTransaction.WaitForCommitAsync()
                // should throw corresponding exception.
                var exception = await Assert.ThrowsAsync<NonRecoverableException>(
                    async () => await transaction.WaitForCommitAsync());

                Assert.True(exception.Status.IsIllegalState);
                Assert.Equal("transaction is still open", exception.Message);

                await harness.KillAllMasterServersAsync();
                await harness.StartAllMasterServersAsync();

                // It should be possible to commit the transaction.
                await transaction.CommitAsync();
                await transaction.WaitForCommitAsync();

                // An extra sanity check: read back the rows written into the table in the
                // context of the transaction.
                var scanner = client.NewScanBuilder(table)
                    .SetReadMode(ReadMode.ReadYourWrites)
                    .SetReplicaSelection(ReplicaSelection.LeaderOnly)
                    .Build();

                Assert.Equal(1, await ClientTestUtil.CountRowsInScanAsync(scanner));
            }

            // Similar to the above, but run KuduTransaction.commit() when only 2 out
            // of 3 masters are running while the TxnManager which used to start the
            // transaction is no longer around.
            using (var transaction = await client.NewTransactionAsync())
            {
                var insert = ClientTestUtil.CreateBasicSchemaInsert(table, 1);
                await transaction.WriteAsync(new[] { insert });

                await harness.KillLeaderMasterServerAsync();

                // It should be possible to commit the transaction: 2 out of 3 masters are
                // running and Raft should be able to establish a leader master. So,
                // txn-related operations routed through TxnManager should succeed.
                await transaction.CommitAsync();
                await transaction.WaitForCommitAsync();

                // An extra sanity check: read back the rows written into the table in the
                // context of the transaction.
                var scanner = client.NewScanBuilder(table)
                    .SetReadMode(ReadMode.ReadYourWrites)
                    .SetReplicaSelection(ReplicaSelection.LeaderOnly)
                    .Build();

                // 1 row should be there from earlier, plus the one we just committed.
                Assert.Equal(2, await ClientTestUtil.CountRowsInScanAsync(scanner));
            }
        }

        private static async Task CommitAndVerifyTransactionAsync(
            KuduClient client, KuduTransaction transaction)
        {
            var tsBeforeCommit = client.LastPropagatedTimestamp;
            await transaction.CommitAsync();
            Assert.Equal(tsBeforeCommit, client.LastPropagatedTimestamp);

            await transaction.WaitForCommitAsync();
            var tsAfterCommit = client.LastPropagatedTimestamp;
            Assert.True(tsAfterCommit > tsBeforeCommit);

            // A sanity check: calling WaitForCommitAsync() again after the commit phase
            // has been finalized doesn't change last propagated timestamp at the
            // client side.
            for (int i = 0; i < 10; ++i)
            {
                await transaction.WaitForCommitAsync();
                Assert.Equal(tsAfterCommit, client.LastPropagatedTimestamp);
                await Task.Delay(10);
            }
        }

        private static KuduTransaction MakeFakeTransaction(
            KuduClient client, KuduTransaction transaction)
        {
            var buf = transaction.Serialize();
            var pb = TxnTokenPB.Parser.ParseFrom(buf);
            Assert.True(pb.HasTxnId);
            var txnId = pb.TxnId;
            Assert.True(txnId > KuduClient.InvalidTxnId);

            var fakeTxnId = txnId + 123;
            var message = new TxnTokenPB
            {
                TxnId = fakeTxnId,
                EnableKeepalive = false,
                KeepaliveMillis = 0
            };

            var fakeTxnBuf = ProtobufHelper.ToByteArray(message);
            return client.NewTransactionFromToken(fakeTxnBuf);
        }
    }
}
