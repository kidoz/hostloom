using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Text.Json;
using HostLoom.Redis;
using StackExchange.Redis;
using Xunit;

namespace HostLoom.IntegrationTests.Infrastructure;

/// <summary>Controls only the runner's six-process, loopback-bound disposable cluster.</summary>
internal sealed class RedisClusterFixture
{
    internal const string Variable = "HOSTLOOM_REDIS_CLUSTER_FIXTURE";
    private readonly string _container;
    private readonly string _owner;
    private readonly int[] _ports;
    private readonly string? _gatewayContainer;
    public int? GatewayPort { get; }

    public RedisClusterFixture()
    {
        var path =
            Environment.GetEnvironmentVariable(Variable)
            ?? throw new InvalidOperationException("Run scripts/test-redis-cluster.py first.");
        using var document = JsonDocument.Parse(File.ReadAllText(path));
        var data = document.RootElement;
        _container = data.GetProperty("container").GetString()!;
        _owner = data.GetProperty("owner").GetString()!;
        _ports = data.GetProperty("ports")
            .EnumerateArray()
            .Select(port => port.GetInt32())
            .ToArray();
        if (
            data.TryGetProperty("gatewayPort", out var gatewayPort)
            && gatewayPort.ValueKind == JsonValueKind.Number
        )
        {
            GatewayPort = gatewayPort.GetInt32();
            _gatewayContainer = data.GetProperty("gatewayContainer").GetString();
            if (
                GatewayPort is < 1 or > 65535
                || _ports.Contains(GatewayPort.Value)
                || _gatewayContainer is not { Length: 64 }
                || !_gatewayContainer.All(char.IsAsciiHexDigit)
            )
                throw new InvalidOperationException("Invalid gateway fixture identity.");
        }
        if (
            _container.Length != 64
            || !_container.All(char.IsAsciiHexDigit)
            || _owner.Length != 32
            || !_owner.All(char.IsAsciiHexDigit)
            || _ports.Length != 6
            || _ports.Distinct().Count() != 6
            || _ports.Any(port => port is < 1 or > 65535)
        )
            throw new InvalidOperationException("Invalid cluster fixture identity.");
    }

    public RedisOptions Options() =>
        new()
        {
            Configuration = string.Join(',', _ports.Select(port => "127.0.0.1:" + port)),
            UseHashTags = true,
            ConnectTimeout = TimeSpan.FromSeconds(2),
            CommandTimeout = TimeSpan.FromSeconds(2),
            HealthTimeout = TimeSpan.FromMilliseconds(500),
            ClientName = "hostloom-cluster-" + _owner,
        };

    public RedisOptions GatewayOptions()
    {
        var options = Options();
        options.Configuration =
            "127.0.0.1:"
            + (GatewayPort ?? throw new InvalidOperationException("Run with --haproxy."));
        return options;
    }

    public async Task PromoteAsync(ClusterNode replica, CancellationToken token)
    {
        var port = OwnedPort(replica.EndPoint);
        await VerifyOwnershipAsync(token);
        Assert.Equal("OK", (await CliAsync(port, ["CLUSTER", "FAILOVER"], token)).Trim());
        await WaitForPromotionAsync(replica, token);
    }

    public async Task RestartGatewayAsync(CancellationToken token)
    {
        await VerifyGatewayAsync(token);
        await DockerAsync(["restart", "--time", "0", _gatewayContainer!], token);
    }

    public async Task RouteGatewayToReplicaAsync(ClusterNode replica, CancellationToken token)
    {
        var target = OwnedPort(replica.EndPoint);
        await VerifyGatewayAsync(token);
        foreach (var port in _ports)
        {
            await GatewayCommandAsync($"disable health primaries/node{port}", token);
            await GatewayCommandAsync($"set server primaries/node{port} state maint", token);
            await GatewayCommandAsync($"shutdown sessions server primaries/node{port}", token);
        }
        await GatewayCommandAsync($"set server primaries/node{target} health up", token);
        await GatewayCommandAsync($"set server primaries/node{target} state ready", token);
    }

    private async Task GatewayCommandAsync(string command, CancellationToken token)
    {
        var result = await DockerAsync(
            [
                "exec",
                _gatewayContainer!,
                "sh",
                "-c",
                "printf '%s\\n' \"$1\" | nc -w 1 127.0.0.1 20010",
                "hostloom-gateway",
                command,
            ],
            token
        );
        Assert.True(
            string.IsNullOrWhiteSpace(result),
            "HAProxy rejected the fixture command: " + result
        );
    }

    private async Task VerifyGatewayAsync(CancellationToken token)
    {
        await VerifyOwnershipAsync(token);
        using var document = JsonDocument.Parse(
            await DockerAsync(["inspect", _gatewayContainer!], token)
        );
        var data = document.RootElement[0];
        var host = data.GetProperty("HostConfig");
        if (
            data.GetProperty("Id").GetString() != _gatewayContainer
            || data.GetProperty("Name").GetString() != "/hostloom-redis-gateway-" + _owner
            || data.GetProperty("Config")
                .GetProperty("Labels")
                .GetProperty("hostloom.redis.cluster-owner")
                .GetString() != _owner
            || host.GetProperty("NetworkMode").GetString() != "container:" + _container
            || host.GetProperty("Memory").GetInt64() != 64 * 1024 * 1024
            || host.GetProperty("NanoCpus").GetInt64() != 1_000_000_000
        )
            throw new InvalidOperationException("Gateway ownership or containment mismatch.");
    }

    public async Task<(
        string Namespace,
        ClusterNode Primary,
        ClusterNode Replica
    )> SelectShardAsync(RedisConnection connection, int shard, CancellationToken token)
    {
        var mux = await connection.GetMultiplexerAsync(token);
        ClusterConfiguration? topology = null;
        await PollAsync(
            async () =>
            {
                topology = await mux.GetServer("127.0.0.1", _ports[0])
                    .ClusterNodesAsync()
                    .WaitAsync(token);
                return topology is not null
                    && topology.Nodes.Count == 6
                    && topology.Nodes.All(node =>
                        node.IsConnected && !node.IsFail && !node.IsPossiblyFail
                    );
            },
            token
        );
        Assert.NotNull(topology);
        var primaries = topology
            .Nodes.Where(node => !node.IsReplica && node.Slots.Count > 0)
            .OrderBy(node => Port(node.EndPoint))
            .ToArray();
        Assert.Equal(3, primaries.Length);
        Assert.Equal(6, topology.Nodes.Count);
        var primary = topology.GetBySlot(shard * 6000);
        Assert.NotNull(primary);
        var replica = Assert.Single(topology.Nodes, node => node.ParentNodeId == primary.NodeId);
        var prefix = "cluster-" + Guid.NewGuid().ToString("N");
        for (var attempt = 0; attempt < 1000; attempt++)
        {
            var ns = prefix + "-" + attempt;
            if (
                topology.GetBySlot((RedisKey)("{" + ns + "}:cache:data:catalog"))?.NodeId
                == primary.NodeId
            )
                return (ns, primary, replica);
        }
        throw new InvalidOperationException(
            "Could not choose a namespace for the requested shard."
        );
    }

    public async Task CrashAsync(ClusterNode node, CancellationToken token)
    {
        var port = OwnedPort(node.EndPoint);
        await VerifyOwnershipAsync(token);
        // The private pidfile belongs to this exact runner-created node configuration.
        await DockerAsync(
            [
                "exec",
                _container,
                "sh",
                "-c",
                "kill -KILL \"$(cat \"$1\")\"",
                "hostloom-node",
                $"/data/node-{port}/redis.pid",
            ],
            token
        );
        TestContext.Current.TestOutputHelper!.WriteLine(
            $"Killed primary {node.NodeId} at 127.0.0.1:{port}."
        );
    }

    public async Task RestartAsync(ClusterNode node, CancellationToken token)
    {
        var port = OwnedPort(node.EndPoint);
        await VerifyOwnershipAsync(token);
        await DockerAsync(
            ["exec", _container, "redis-server", $"/fixture/node-{port}.conf"],
            token
        );
        await PollAsync(
            async () =>
            {
                var info = await CliAsync(port, ["INFO", "replication"], token);
                return info.Contains("role:slave", StringComparison.Ordinal)
                    && info.Contains("master_link_status:up", StringComparison.Ordinal);
            },
            token
        );
        TestContext.Current.TestOutputHelper!.WriteLine(
            $"Former primary {node.NodeId} rejoined as a connected replica."
        );
    }

    public async Task WaitForPromotionAsync(ClusterNode replica, CancellationToken token)
    {
        var started = Stopwatch.GetTimestamp();
        var port = OwnedPort(replica.EndPoint);
        await PollAsync(
            async () =>
            {
                var info = await CliAsync(port, ["INFO", "replication"], token);
                var cluster = await CliAsync(port, ["CLUSTER", "INFO"], token);
                return info.Contains("role:master", StringComparison.Ordinal)
                    && cluster.Contains("cluster_state:ok", StringComparison.Ordinal)
                    && cluster.Contains("cluster_slots_ok:16384", StringComparison.Ordinal);
            },
            token
        );
        Assert.Equal(replica.NodeId, (await CliAsync(port, ["CLUSTER", "MYID"], token)).Trim());
        TestContext.Current.TestOutputHelper!.WriteLine(
            $"Replica {replica.NodeId} promoted in {Stopwatch.GetElapsedTime(started).TotalMilliseconds:F0} ms; all 16384 slots available."
        );
    }

    public static async Task WaitReplicatedAsync(
        IDatabase db,
        RedisKey key,
        RedisValue value,
        CancellationToken token
    )
    {
        await PollAsync(
            async () =>
                await db.StringGetAsync(key, CommandFlags.DemandReplica).WaitAsync(token) == value,
            token
        );
    }

    public static async Task WaitForClientRoutingAsync(
        RedisConnection connection,
        string key,
        ClusterNode promoted,
        CancellationToken token
    )
    {
        var db = await connection.GetDatabaseAsync(token);
        var started = Stopwatch.GetTimestamp();
        await PollAsync(
            async () =>
            {
                try
                {
                    return Equals(
                        promoted.EndPoint,
                        await db.IdentifyEndpointAsync(key, CommandFlags.DemandMaster)
                            .WaitAsync(token)
                    );
                }
                catch (RedisException)
                {
                    return false;
                }
            },
            token
        );
        TestContext.Current.TestOutputHelper!.WriteLine(
            $"Client route to promoted node converged in {Stopwatch.GetElapsedTime(started).TotalMilliseconds:F0} ms."
        );
    }

    public static async Task PollAsync(Func<Task<bool>> condition, CancellationToken token)
    {
        var started = Stopwatch.GetTimestamp();
        var deadline = DateTimeOffset.UtcNow + TimeSpan.FromSeconds(30);
        while (true)
        {
            CheckDeadline();
            var satisfied = await condition();
            // A late successful response (including after the host resumes from sleep)
            // must not turn a missed recovery deadline into a passing assertion.
            CheckDeadline();
            if (satisfied)
                return;
            await Task.Delay(100, token);
        }

        void CheckDeadline()
        {
            token.ThrowIfCancellationRequested();
            if (
                DateTimeOffset.UtcNow > deadline
                || Stopwatch.GetElapsedTime(started) > TimeSpan.FromSeconds(30)
            )
                throw new TimeoutException(
                    "Redis Cluster did not reach the required state within 30 seconds."
                );
        }
    }

    private Task<string> CliAsync(int port, string[] arguments, CancellationToken token) =>
        DockerAsync(
            [
                "exec",
                _container,
                "redis-cli",
                "--raw",
                "-p",
                port.ToString(CultureInfo.InvariantCulture),
                .. arguments,
            ],
            token
        );

    private int OwnedPort(EndPoint? endpoint)
    {
        var port = Port(endpoint);
        if (!_ports.Contains(port))
            throw new InvalidOperationException("Node is outside the owned cluster.");
        return port;
    }

    private static int Port(EndPoint? endpoint) =>
        endpoint switch
        {
            IPEndPoint ip when IPAddress.IsLoopback(ip.Address) => ip.Port,
            DnsEndPoint dns when dns.Host == "127.0.0.1" => dns.Port,
            _ => throw new InvalidOperationException(
                "The cluster must advertise only loopback addresses."
            ),
        };

    private async Task VerifyOwnershipAsync(CancellationToken token)
    {
        using var document = JsonDocument.Parse(await DockerAsync(["inspect", _container], token));
        var data = document.RootElement[0];
        var host = data.GetProperty("HostConfig");
        var bindings = host.GetProperty("PortBindings");
        if (
            data.GetProperty("Id").GetString() != _container
            || data.GetProperty("Name").GetString() != "/hostloom-redis-cluster-" + _owner
            || data.GetProperty("Config")
                .GetProperty("Labels")
                .GetProperty("hostloom.redis.cluster-owner")
                .GetString() != _owner
            || host.GetProperty("Memory").GetInt64() != 512 * 1024 * 1024
            || host.GetProperty("NanoCpus").GetInt64() != 2_000_000_000
            || bindings.EnumerateObject().Count() != (GatewayPort.HasValue ? 7 : 6)
        )
            throw new InvalidOperationException("Cluster ownership or containment mismatch.");
        foreach (var port in GatewayPort is { } gateway ? [.. _ports, gateway] : _ports)
        {
            var binding = bindings.GetProperty(
                port.ToString(CultureInfo.InvariantCulture) + "/tcp"
            );
            if (
                binding.GetArrayLength() != 1
                || binding[0].GetProperty("HostIp").GetString() != "127.0.0.1"
                || binding[0].GetProperty("HostPort").GetString()
                    != port.ToString(CultureInfo.InvariantCulture)
            )
                throw new InvalidOperationException("Cluster port containment mismatch.");
        }
    }

    private static async Task<string> DockerAsync(string[] arguments, CancellationToken token)
    {
        using var bounded = CancellationTokenSource.CreateLinkedTokenSource(token);
        bounded.CancelAfter(TimeSpan.FromSeconds(20));
        var start = new ProcessStartInfo("docker")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        foreach (var argument in arguments)
            start.ArgumentList.Add(argument);
        using var process =
            Process.Start(start) ?? throw new InvalidOperationException("Docker did not start.");
        var output = process.StandardOutput.ReadToEndAsync(bounded.Token);
        var error = process.StandardError.ReadToEndAsync(bounded.Token);
        try
        {
            await process.WaitForExitAsync(bounded.Token);
        }
        catch (OperationCanceledException)
        {
            if (!process.HasExited)
                process.Kill(entireProcessTree: true);
            await process.WaitForExitAsync(CancellationToken.None);
            throw;
        }
        var result = await output;
        var diagnostic = await error;
        if (process.ExitCode != 0)
            throw new InvalidOperationException("Docker cluster operation failed: " + diagnostic);
        return result;
    }
}
