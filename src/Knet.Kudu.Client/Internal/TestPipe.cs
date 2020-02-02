using System;
using System.IO;
using System.IO.Pipelines;
using System.Threading.Tasks;
using Knet.Kudu.Client.Util;

namespace Knet.Kudu.Client.Internal
{
    public sealed class TestPipe : IDuplexPipe
    {
        public static volatile TaskCompletionSource<object> DelayTcs =
            new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);

        public static volatile bool ShouldDelay;

        private readonly Stream _stream;
        private readonly Pipe _readPipe;
        private readonly Pipe _writePipe;
        private readonly string _name;

        public TestPipe(
            Stream stream,
            PipeOptions sendPipeOptions,
            PipeOptions receivePipeOptions,
            string name = null)
        {
            _stream = stream;
            _name = name;

            _readPipe = new Pipe(receivePipeOptions);
            _writePipe = new Pipe(sendPipeOptions);
            _ = CopyFromStreamToReadPipe();
            _ = CopyFromWritePipeToStream();
        }

        public PipeReader Input => _readPipe.Reader;

        public PipeWriter Output => _writePipe.Writer;

        public override string ToString() => _name;

        private async Task CopyFromStreamToReadPipe()
        {
            var stream = _stream;
            var writer = _readPipe.Writer;

            try
            {
                while (true)
                {
                    var memory = writer.GetMemory();

                    var read = await stream.ReadAsync(memory).ConfigureAwait(false);

                    if (read <= 0)
                        break;

                    writer.Advance(read);
                    await writer.FlushAsync().ConfigureAwait(false);
                }

                await writer.CompleteAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                await writer.CompleteAsync(ex).ConfigureAwait(false);
            }
        }

        private async Task CopyFromWritePipeToStream()
        {
            var stream = _stream;
            var reader = _writePipe.Reader;

            try
            {
                while (true)
                {
                    var result = await reader.ReadAsync().ConfigureAwait(false);
                    var buffer = result.Buffer;

                    if (result.IsCanceled)
                        break;

                    if (buffer.IsEmpty && result.IsCompleted)
                        break;

                    Console.WriteLine($"Sending network buffer: {buffer.Length} bytes");

                    foreach (var segment in buffer)
                    {
                        await stream.WriteAsync(segment).ConfigureAwait(false);
                        await stream.FlushAsync().ConfigureAwait(false);
                    }

                    reader.AdvanceTo(buffer.End);

                    if (ShouldDelay)
                    {
                        // Delay the next write so we're guaranteed to group
                        // multiple RPCs into a single network request.
                        await DelayTcs.Task.ConfigureAwait(false);
                    }
                }

                await reader.CompleteAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                await reader.CompleteAsync(ex).ConfigureAwait(false);
            }
        }
    }
}
