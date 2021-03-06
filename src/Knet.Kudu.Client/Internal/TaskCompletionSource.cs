#if !NET5_0_OR_GREATER

using System.Threading.Tasks;

namespace Knet.Kudu.Client.Internal
{
    internal class TaskCompletionSource : TaskCompletionSource<object>
    {
        public TaskCompletionSource() : base() { }

        public TaskCompletionSource(object state) : base(state) { }

        public TaskCompletionSource(TaskCreationOptions creationOptions) : base(creationOptions) { }

        public TaskCompletionSource(object state, TaskCreationOptions creationOptions)
            : base(state, creationOptions) { }

        public void SetResult()
        {
            SetResult(null);
        }

        public bool TrySetResult()
        {
            return TrySetResult(null);
        }
    }
}

#endif
