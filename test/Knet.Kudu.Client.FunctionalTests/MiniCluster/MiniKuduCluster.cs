using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Knet.Kudu.Binary;
using Knet.Kudu.Client.Connection;
using Knet.Kudu.Client.Internal;
using Knet.Kudu.Client.Protobuf.Tools;

namespace Knet.Kudu.Client.FunctionalTests.MiniCluster;

public class MiniKuduCluster : IAsyncDisposable
{
    // Hack-fix to avoid resource issues on CI.
    private static readonly int _maxConcurrentClusters = Math.Max(4, Environment.ProcessorCount);
    private static readonly SemaphoreSlim _testLimiter = new(
        _maxConcurrentClusters, _maxConcurrentClusters);

    private readonly CreateClusterRequestPB _createClusterRequestPB;
    private readonly Dictionary<HostAndPort, DaemonInfo> _masterServers;
    private readonly Dictionary<HostAndPort, DaemonInfo> _tabletServers;
    private readonly SemaphoreSlim _singleRequest;

    private Process _nativeProcess;
    private Task _readStdErrTask;

    public MiniKuduCluster(CreateClusterRequestPB createClusterRequestPB)
    {
        _createClusterRequestPB = createClusterRequestPB;
        _masterServers = new Dictionary<HostAndPort, DaemonInfo>();
        _tabletServers = new Dictionary<HostAndPort, DaemonInfo>();
        _singleRequest = new SemaphoreSlim(1, 1);
    }

    private Stream StdIn => _nativeProcess.StandardInput.BaseStream;

    private Stream StdOut => _nativeProcess.StandardOutput.BaseStream;

    private Stream StdErr => _nativeProcess.StandardError.BaseStream;

    public async Task StartAsync()
    {
        await _testLimiter.WaitAsync();

        var kuduExe = KuduBinaryLocator.FindBinary("kudu");
        var workingDirectory = Path.GetDirectoryName(kuduExe.ExePath);

        var startInfo = new ProcessStartInfo
        {
            FileName = kuduExe.ExePath,
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

        foreach (var env in kuduExe.EnvironmentVariables)
        {
            startInfo.EnvironmentVariables.Add(env.Key, env.Value);
        }

        _nativeProcess = new Process { StartInfo = startInfo };
        _nativeProcess.Start();

        // Start listening for events.
        _readStdErrTask = ReadStdErrAsync();

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
        _testLimiter.Release();

        await StdIn.DisposeAsync();
        await StdOut.DisposeAsync();
        await StdErr.DisposeAsync();
        await _readStdErrTask;

        await _nativeProcess.WaitForExitAsync();
        _nativeProcess.Dispose();
        _singleRequest.Dispose();
    }

    public KuduClient CreateClient()
    {
        return KuduClient.NewBuilder(_masterServers.Keys.ToList()).Build();
    }

    public KuduClientBuilder CreateClientBuilder()
    {
        return KuduClient.NewBuilder(_masterServers.Keys.ToList());
    }

    public List<HostAndPort> GetMasterServers()
    {
        return _masterServers.Keys.ToList();
    }

    public List<HostAndPort> GetTabletServers()
    {
        return _tabletServers.Keys.ToList();
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
        var messageSize = req.CalculateSize();
        var buffer = new byte[messageSize + 4];

        BinaryPrimitives.WriteInt32BigEndian(buffer, messageSize);
        req.WriteTo(buffer.AsSpan(4));

        return SendReceiveAsync(buffer);
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
            var messageSize = BinaryPrimitives.ReadInt32BigEndian(buffer);

            buffer = new byte[messageSize];
            await ReadExactAsync(buffer);

            var response = ControlShellResponsePB.Parser.ParseFrom(buffer);

            if (response.Error is not null)
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
        if (buffer.Length == 0)
            return;

        do
        {
            var bytesReceived = await StdOut.ReadAsync(buffer);

            if (bytesReceived <= 0)
                throw new IOException("StdOut closed");

            buffer = buffer.Slice(bytesReceived);
        } while (buffer.Length > 0);
    }

    private async Task ReadStdErrAsync()
    {
        await using var output = Console.OpenStandardError();

        try
        {
            await StdErr.CopyToAsync(output);
        }
        catch { }
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
