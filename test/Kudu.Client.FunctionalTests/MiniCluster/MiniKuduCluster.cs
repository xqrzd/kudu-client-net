using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using Kudu.Client.Connection;
using Kudu.Client.Internal;
using Kudu.Client.Protocol.Tools;
using Kudu.Client.Util;
using ProtoBuf;

namespace Kudu.Client.FunctionalTests.MiniCluster
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

            // Hack-fix because this client doesn't have fault tolerance yet.
            // A master leader might not have been elected yet.
            Thread.Sleep(2000);
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
            return KuduClient.NewClientBuilder(_masterServers.Keys.ToList()).Build();
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
