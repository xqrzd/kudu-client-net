using System;
using System.Threading.Tasks;
using Knet.Kudu.Client.Connection;
using Xunit;

namespace Knet.Kudu.Client.Tests
{
    public class RequestTrackerTests
    {
        [Fact]
        public void Test()
        {
            var tracker = new RequestTracker("test");

            // A new tracker should have no incomplete RPCs.
            Assert.Equal(RequestTracker.NoSeqNo, tracker.FirstIncomplete);

            int max = 10;

            for (int i = 0; i < max; i++)
            {
                tracker.GetNewSeqNo();
            }

            // The first RPC is the incomplete one.
            Assert.Equal(1, tracker.FirstIncomplete);

            // Mark the first as complete, incomplete should advance by 1.
            tracker.CompleteRpc(1);
            Assert.Equal(2, tracker.FirstIncomplete);

            // Mark the RPC in the middle as complete, first incomplete doesn't change.
            tracker.CompleteRpc(5);
            Assert.Equal(2, tracker.FirstIncomplete);

            // Mark 2-4 inclusive as complete.
            for (int i = 2; i <= 4; i++)
            {
                tracker.CompleteRpc(i);
            }

            Assert.Equal(6, tracker.FirstIncomplete);

            // Get a few more sequence numbers.
            long lastSeqNo = 0;
            for (int i = max / 2; i <= max; i++)
            {
                lastSeqNo = tracker.GetNewSeqNo();
            }

            // Mark them all as complete except the last one.
            while (tracker.FirstIncomplete != lastSeqNo)
            {
                tracker.CompleteRpc(tracker.FirstIncomplete);
            }

            Assert.Equal(lastSeqNo, tracker.FirstIncomplete);
            tracker.CompleteRpc(lastSeqNo);

            // Test that we get back to NO_SEQ_NO after marking them all.
            Assert.Equal(RequestTracker.NoSeqNo, tracker.FirstIncomplete);
        }

        [Fact]
        public async Task TestMultiThreaded()
        {
            var rt = new RequestTracker("fake id");
            var checker = new Checker();
            var tasks = new Task[16];

            for (int i = 0; i < tasks.Length; i++)
            {
                tasks[i] = Task.Run(() =>
                {
                    for (int n = 0; n < 1000; n++)
                    {
                        long seqNo = rt.GetNewSeqNo();
                        long incomplete = rt.FirstIncomplete;
                        checker.Check(seqNo, incomplete);
                        rt.CompleteRpc(seqNo);
                    }
                });
            }

            await Task.WhenAll(tasks);
        }

        private class Checker
        {
            private long _curIncomplete = 0;

            public void Check(long seqNo, long firstIncomplete)
            {
                lock (this)
                {
                    Assert.True(seqNo >= _curIncomplete,
                        "should not send a seq number that was previously marked complete");
                    _curIncomplete = Math.Max(firstIncomplete, _curIncomplete);
                }
            }
        }
    }
}
