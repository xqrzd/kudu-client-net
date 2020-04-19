using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Knet.Kudu.Client.Connection;
using Knet.Kudu.Client.Protocol.Tools;
using Knet.Kudu.Client.Util;
using ProtoBuf;

namespace Knet.Kudu.Client.FunctionalTests.MiniCluster
{
    public class AsyncMiniKuduCluster : IAsyncDisposable
    {
        private readonly CreateClusterRequestPB _createClusterRequestPB;
        private readonly Dictionary<HostAndPort, DaemonInfo> _masterServers;
        private readonly Dictionary<HostAndPort, DaemonInfo> _tabletServers;
        private readonly SemaphoreSlim _singleRequest;

        private ProcessEx _nativeProcess;

        public AsyncMiniKuduCluster(CreateClusterRequestPB createClusterRequestPB)
        {
            _createClusterRequestPB = createClusterRequestPB;
            _masterServers = new Dictionary<HostAndPort, DaemonInfo>();
            _tabletServers = new Dictionary<HostAndPort, DaemonInfo>();
            _singleRequest = new SemaphoreSlim(1, 1);
        }

        private Stream StdIn => _nativeProcess.StdIn;

        private Stream StdOut => _nativeProcess.StdOut;

        private Stream StdErr => _nativeProcess.StdErr;

        public async Task StartAsync()
        {
            Directory.CreateDirectory(_createClusterRequestPB.ClusterRoot);

            var kuduExe = KuduBinaryLocator.FindBinary("kudu");
            var workingDirectory = Path.GetDirectoryName(kuduExe);

            var startInfo = new ProcessStartInfo
            {
                FileName = kuduExe,
                WorkingDirectory = workingDirectory,
                RedirectStandardInput = true,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true
            };

            startInfo.ArgumentList.Add("test");
            startInfo.ArgumentList.Add("mini_cluster");
            startInfo.ArgumentList.Add("--serialization=pb");

            _nativeProcess = new ProcessEx(startInfo);
            _nativeProcess.Start();

            // Start listening for events.
            _ = ReadStdErrAsync();

            var createClusterRequest = new ControlShellRequestPB
            {
                CreateCluster = _createClusterRequestPB
            };
            await SendRequestToClusterAsync(createClusterRequest);

            var startClusterRequest = new ControlShellRequestPB
            {
                StartCluster = new StartClusterRequestPB()
            };
            await SendRequestToClusterAsync(startClusterRequest);

            // Initialize the maps of master and tablet servers.
            var getMastersRequest = new ControlShellRequestPB
            {
                GetMasters = new GetMastersRequestPB()
            };
            var masters = await SendRequestToClusterAsync(getMastersRequest);

            foreach (var master in masters.GetMasters.Masters)
            {
                var hostPort = master.BoundRpcAddress.ToHostAndPort();
                var daemonInfo = new DaemonInfo(master.Id, true);
                _masterServers.Add(hostPort, daemonInfo);
            }

            var getTabletsRequest = new ControlShellRequestPB
            {
                GetTservers = new GetTServersRequestPB()
            };
            var tablets = await SendRequestToClusterAsync(getTabletsRequest);

            foreach (var tserver in tablets.GetTservers.Tservers)
            {
                var hostPort = tserver.BoundRpcAddress.ToHostAndPort();
                var daemonInfo = new DaemonInfo(tserver.Id, true);
                _tabletServers.Add(hostPort, daemonInfo);
            }
        }

        public async ValueTask DisposeAsync()
        {
            await StdIn.DisposeAsync();
            await StdOut.DisposeAsync();
            await StdErr.DisposeAsync();

            await _nativeProcess.WaitUntilExitAsync();
            _nativeProcess.Dispose();
            _singleRequest.Dispose();

            try
            {
                Directory.Delete(_createClusterRequestPB.ClusterRoot, true);
            }
            catch { }
        }

        public KuduClient CreateClient()
        {
            return KuduClient.NewBuilder(_masterServers.Keys.ToList()).Build();
        }

        public KuduClientBuilder CreateClientBuilder()
        {
            return KuduClient.NewBuilder(_masterServers.Keys.ToList());
        }

        /// <summary>
        /// Starts a master server identified by a host and port.
        /// Does nothing if the server was already running.
        /// </summary>
        /// <param name="hostPort">Unique host and port identifying the server.</param>
        public async Task StartMasterServerAsync(HostAndPort hostPort)
        {
            var daemonInfo = GetMasterServer(hostPort);
            if (daemonInfo.IsRunning)
                return;

            var request = new ControlShellRequestPB
            {
                StartDaemon = new StartDaemonRequestPB { Id = daemonInfo.Id }
            };
            await SendRequestToClusterAsync(request);

            daemonInfo.IsRunning = true;
        }

        /// <summary>
        /// Kills a master server identified identified by an host and port.
        /// Does nothing if the master was already dead.
        /// </summary>
        /// <param name="hostPort">Unique host and port identifying the server.</param>
        public async Task KillMasterServerAsync(HostAndPort hostPort)
        {
            var daemonInfo = GetMasterServer(hostPort);
            if (!daemonInfo.IsRunning)
                return;

            var request = new ControlShellRequestPB
            {
                StopDaemon = new StopDaemonRequestPB { Id = daemonInfo.Id }
            };
            await SendRequestToClusterAsync(request);

            daemonInfo.IsRunning = false;
        }

        /// <summary>
        /// Starts a tablet server identified by a host and port.
        /// Does nothing if the server was already running.
        /// </summary>
        /// <param name="hostPort">Unique host and port identifying the server.</param>
        public async Task StartTabletServerAsync(HostAndPort hostPort)
        {
            var daemonInfo = GetTabletServer(hostPort);
            if (daemonInfo.IsRunning)
                return;

            var request = new ControlShellRequestPB
            {
                StartDaemon = new StartDaemonRequestPB { Id = daemonInfo.Id }
            };
            await SendRequestToClusterAsync(request);

            daemonInfo.IsRunning = true;
        }

        /// <summary>
        /// Kills a tablet server identified identified by an host and port.
        /// Does nothing if the server was already dead.
        /// </summary>
        /// <param name="hostPort">Unique host and port identifying the server.</param>
        public async Task KillTabletServerAsync(HostAndPort hostPort)
        {
            var daemonInfo = GetTabletServer(hostPort);
            if (!daemonInfo.IsRunning)
                return;

            var request = new ControlShellRequestPB
            {
                StopDaemon = new StopDaemonRequestPB { Id = daemonInfo.Id }
            };
            await SendRequestToClusterAsync(request);

            daemonInfo.IsRunning = false;
        }

        /// <summary>
        /// Starts all the master servers.
        /// Does nothing to the servers that are already running.
        /// </summary>
        public async Task StartAllMasterServersAsync()
        {
            foreach (var hostPort in _masterServers.Keys)
                await StartMasterServerAsync(hostPort);
        }

        /// <summary>
        /// Kills all the master servers.
        /// Does nothing to the servers that are already dead.
        /// </summary>
        public async Task KillAllMasterServersAsync()
        {
            foreach (var hostPort in _masterServers.Keys)
                await KillMasterServerAsync(hostPort);
        }

        /// <summary>
        /// Starts all the tablet servers.
        /// Does nothing to the servers that are already running.
        /// </summary>
        public async Task StartAllTabletServersAsync()
        {
            foreach (var hostPort in _tabletServers.Keys)
                await StartTabletServerAsync(hostPort);
        }

        /// <summary>
        /// Kills all the tablet servers.
        /// Does nothing to the servers that are already dead.
        /// </summary>
        public async Task KillAllTabletServersAsync()
        {
            foreach (var hostPort in _tabletServers.Keys)
                await KillTabletServerAsync(hostPort);
        }

        // TODO: Set daemon flags (when Kudu 1.11 proto files are generated).

        /// <summary>
        /// Returns a master server identified by an address.
        /// </summary>
        /// <param name="hostPort">Unique host and port identifying the server.</param>
        private DaemonInfo GetMasterServer(HostAndPort hostPort)
        {
            if (_masterServers.TryGetValue(hostPort, out var info))
                return info;

            throw new Exception($"Master server {hostPort} not found");
        }

        /// <summary>
        /// Returns a tablet server identified by an address.
        /// </summary>
        /// <param name="hostPort">Unique host and port identifying the server.</param>
        private DaemonInfo GetTabletServer(HostAndPort hostPort)
        {
            if (_tabletServers.TryGetValue(hostPort, out var info))
                return info;

            throw new Exception($"Tablet server {hostPort} not found");
        }

        private Task<ControlShellResponsePB> SendRequestToClusterAsync(ControlShellRequestPB req)
        {
            var writer = new ArrayBufferWriter<byte>();
            Serializer.Serialize(writer, req);
            var messageLength = writer.WrittenCount;

            var finalWriter = new ArrayBufferWriter<byte>();
            var lengthSpan = finalWriter.GetSpan(4);
            BinaryPrimitives.WriteInt32BigEndian(lengthSpan, messageLength);
            finalWriter.Advance(4);

            var span = finalWriter.GetSpan(messageLength);
            writer.WrittenSpan.CopyTo(span);
            finalWriter.Advance(messageLength);

            var request = finalWriter.WrittenMemory;

            return SendReceiveAsync(request);
        }

        private async Task<ControlShellResponsePB> SendReceiveAsync(ReadOnlyMemory<byte> request)
        {
            await _singleRequest.WaitAsync();

            try
            {
                await StdIn.WriteAsync(request);
                await StdIn.FlushAsync();

                var buffer = new byte[4];
                await ReadExactAsync(buffer);
                var messageLength = BinaryPrimitives.ReadInt32BigEndian(buffer);

                buffer = new byte[messageLength];
                await ReadExactAsync(buffer);

                var ms = new MemoryStream(buffer);
                var response = Serializer.Deserialize<ControlShellResponsePB>(ms);

                if (response.Error != null)
                    throw new IOException(response.Error.Message);

                return response;
            }
            finally
            {
                _singleRequest.Release();
            }
        }

        private async Task ReadExactAsync(Memory<byte> buffer)
        {
            do
            {
                var read = await StdOut.ReadAsync(buffer);
                buffer = buffer.Slice(read);
            } while (buffer.Length > 0);
        }

        private async Task ReadStdErrAsync()
        {
            var buffer = new byte[4096];

            while (true)
            {
                int read = await StdErr.ReadAsync(buffer);
                var text = Encoding.UTF8.GetString(buffer, 0, read);
                Console.Error.WriteLine(text);
            }
        }

        private sealed class DaemonInfo
        {
            public DaemonIdentifierPB Id { get; }

            public bool IsRunning { get; set; }

            public DaemonInfo(DaemonIdentifierPB id, bool isRunning)
            {
                Id = id;
                IsRunning = isRunning;
            }
        }
    }
}
