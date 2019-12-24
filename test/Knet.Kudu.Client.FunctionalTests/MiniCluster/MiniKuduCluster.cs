using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Knet.Kudu.Client.Connection;
using Knet.Kudu.Client.Internal;
using Knet.Kudu.Client.Protocol.Tools;
using Knet.Kudu.Client.Util;
using ProtoBuf;

namespace Knet.Kudu.Client.FunctionalTests.MiniCluster
{
    public class MiniKuduCluster : IDisposable
    {
        private readonly CreateClusterRequestPB _createClusterRequestPB;

        // Control shell process.
        private Process _miniCluster;

        // Request channel to the control shell.
        private Stream _miniClusterStdin;

        // Response channel from the control shell.
        private Stream _miniClusterStdout;

        private readonly Dictionary<HostAndPort, DaemonInfo> _masterServers;
        private readonly Dictionary<HostAndPort, DaemonInfo> _tabletServers;

        public MiniKuduCluster(CreateClusterRequestPB createClusterRequestPB)
        {
            _createClusterRequestPB = createClusterRequestPB;

            _masterServers = new Dictionary<HostAndPort, DaemonInfo>();
            _tabletServers = new Dictionary<HostAndPort, DaemonInfo>();
        }

        public void Start()
        {
            Directory.CreateDirectory(_createClusterRequestPB.ClusterRoot);

            // TODO: Redirect standard error, and use async reads.
            var kuduExe = KuduBinaryLocator.FindBinary("kudu");
            _miniCluster = new Process();
            _miniCluster.StartInfo.WorkingDirectory = Path.GetDirectoryName(kuduExe);
            _miniCluster.StartInfo.UseShellExecute = false;
            _miniCluster.StartInfo.RedirectStandardInput = true;
            _miniCluster.StartInfo.RedirectStandardOutput = true;
            _miniCluster.StartInfo.FileName = kuduExe;
            _miniCluster.StartInfo.ArgumentList.Add("test");
            _miniCluster.StartInfo.ArgumentList.Add("mini_cluster");
            _miniCluster.StartInfo.ArgumentList.Add("--serialization=pb");

            _miniCluster.Start();

            _miniClusterStdin = _miniCluster.StandardInput.BaseStream;
            _miniClusterStdout = _miniCluster.StandardOutput.BaseStream;

            var createClusterRequest = new ControlShellRequestPB
            {
                CreateCluster = _createClusterRequestPB
            };

            SendRequestToCluster(createClusterRequest);

            var startClusterRequest = new ControlShellRequestPB
            {
                StartCluster = new StartClusterRequestPB()
            };
            SendRequestToCluster(startClusterRequest);

            // Initialize the maps of master and tablet servers.
            var getMastersRequest = new ControlShellRequestPB
            {
                GetMasters = new GetMastersRequestPB()
            };
            var masters = SendRequestToCluster(getMastersRequest);

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
            var tablets = SendRequestToCluster(getTabletsRequest);

            foreach (var tserver in tablets.GetTservers.Tservers)
            {
                var hostPort = tserver.BoundRpcAddress.ToHostAndPort();
                var daemonInfo = new DaemonInfo(tserver.Id, true);
                _tabletServers.Add(hostPort, daemonInfo);
            }
        }

        public void Shutdown()
        {
            _miniClusterStdin.Close();
            _miniClusterStdout.Close();
            _miniCluster.WaitForExit();
        }

        public void Dispose()
        {
            Shutdown();

            _miniClusterStdin.Dispose();
            _miniClusterStdout.Dispose();
            _miniCluster.Dispose();

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

        /// <summary>
        /// Starts a master server identified by a host and port.
        /// Does nothing if the server was already running.
        /// </summary>
        /// <param name="hostPort">Unique host and port identifying the server.</param>
        public void StartMasterServer(HostAndPort hostPort)
        {
            var daemonInfo = GetMasterServer(hostPort);
            if (daemonInfo.IsRunning)
                return;

            var request = new ControlShellRequestPB
            {
                StartDaemon = new StartDaemonRequestPB { Id = daemonInfo.Id }
            };
            SendRequestToCluster(request);

            daemonInfo.IsRunning = true;
        }

        /// <summary>
        /// Kills a master server identified identified by an host and port.
        /// Does nothing if the master was already dead.
        /// </summary>
        /// <param name="hostPort">Unique host and port identifying the server.</param>
        public void KillMasterServer(HostAndPort hostPort)
        {
            var daemonInfo = GetMasterServer(hostPort);
            if (!daemonInfo.IsRunning)
                return;

            var request = new ControlShellRequestPB
            {
                StopDaemon = new StopDaemonRequestPB { Id = daemonInfo.Id }
            };
            SendRequestToCluster(request);

            daemonInfo.IsRunning = false;
        }

        /// <summary>
        /// Starts a tablet server identified by a host and port.
        /// Does nothing if the server was already running.
        /// </summary>
        /// <param name="hostPort">Unique host and port identifying the server.</param>
        public void StartTabletServer(HostAndPort hostPort)
        {
            var daemonInfo = GetTabletServer(hostPort);
            if (daemonInfo.IsRunning)
                return;

            var request = new ControlShellRequestPB
            {
                StartDaemon = new StartDaemonRequestPB { Id = daemonInfo.Id }
            };
            SendRequestToCluster(request);

            daemonInfo.IsRunning = true;
        }

        /// <summary>
        /// Kills a tablet server identified identified by an host and port.
        /// Does nothing if the master was already dead.
        /// </summary>
        /// <param name="hostPort">Unique host and port identifying the server.</param>
        public void KillTabletServer(HostAndPort hostPort)
        {
            var daemonInfo = GetTabletServer(hostPort);
            if (!daemonInfo.IsRunning)
                return;

            var request = new ControlShellRequestPB
            {
                StopDaemon = new StopDaemonRequestPB { Id = daemonInfo.Id }
            };
            SendRequestToCluster(request);

            daemonInfo.IsRunning = false;
        }

        /// <summary>
        /// Starts all the master servers.
        /// Does nothing to the servers that are already running.
        /// </summary>
        public void StartAllMasterServers()
        {
            foreach (var hostPort in _masterServers.Keys)
                StartMasterServer(hostPort);
        }

        /// <summary>
        /// Kills all the master servers.
        /// Does nothing to the servers that are already dead.
        /// </summary>
        public void KillAllMasterServers()
        {
            foreach (var hostPort in _masterServers.Keys)
                KillMasterServer(hostPort);
        }

        /// <summary>
        /// Starts all the tablet servers.
        /// Does nothing to the servers that are already running.
        /// </summary>
        public void StartAllTabletServers()
        {
            foreach (var hostPort in _tabletServers.Keys)
                StartTabletServer(hostPort);
        }

        /// <summary>
        /// Kills all the tablet servers.
        /// Does nothing to the servers that are already dead.
        /// </summary>
        public void KillAllTabletServers()
        {
            foreach (var hostPort in _tabletServers.Keys)
                KillTabletServer(hostPort);
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

        private ControlShellResponsePB SendRequestToCluster(ControlShellRequestPB req)
        {
            using (var stream = new RecyclableMemoryStream())
            {
                // Make space to write the length of the entire message.
                stream.GetMemory(4);

                //Serializer.SerializeWithLengthPrefix(stream, req, PrefixStyle.Base128);
                Serializer.Serialize(stream, req);

                // Go back and write the length of the entire message, minus the 4
                // bytes we already allocated to store the length.
                BinaryPrimitives.WriteUInt32BigEndian(stream.AsSpan(), (uint)stream.Length - 4);

                _miniClusterStdin.Write(stream.AsSpan());
                _miniClusterStdin.Flush();
            }

            return Receive();
        }

        private ControlShellResponsePB Receive()
        {
            var buffer = new byte[4];
            ReadExact(buffer);
            var messageLength = BinaryPrimitives.ReadInt32BigEndian(buffer);

            buffer = new byte[messageLength];
            ReadExact(buffer);

            var ms = new MemoryStream(buffer);
            var response = Serializer.Deserialize<ControlShellResponsePB>(ms);

            if (response.Error != null)
                throw new IOException(response.Error.Message);

            return response;
        }

        private void ReadExact(Span<byte> buffer)
        {
            do
            {
                var read = _miniClusterStdout.Read(buffer);
                buffer = buffer.Slice(read);
            } while (buffer.Length > 0);
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
