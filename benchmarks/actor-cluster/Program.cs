using System.Net;
using Akka.Actor;
using Akka.Cluster;
using Akka.Cluster.Sharding;
using Akka.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Orleans.Configuration;
using Orleans.Runtime;
using StackExchange.Redis;

namespace ActorClusterTrial;

public interface ICatalog : IGrainWithStringKey
{
    Task<string> RunAsync(string operation);
}

internal sealed class CatalogState(IDatabase database)
{
    private static readonly string Commit = File.ReadAllText(
        Path.Combine(AppContext.BaseDirectory, "commit.lua")
    );
    private readonly string _token =
        $"{Environment.GetEnvironmentVariable("NODE")}:{Guid.NewGuid():N}";

    internal static Task<RedisResult> CommitAsync(
        IDatabase database,
        string token,
        string operation
    ) =>
        database.ScriptEvaluateAsync(
            Commit,
            ["writer", "operations", "journal"],
            [token, operation]
        );

    internal async Task<string> ExecuteAsync(string operation)
    {
        if (operation == "read")
        {
            var journal = await database.ListRangeAsync("journal").ConfigureAwait(false);
            return $"{_token}|{string.Join(",", journal.Select(value => value.ToString()))}";
        }
        var outcome = await CommitAsync(database, _token, operation).ConfigureAwait(false);
        return $"{_token}|{outcome}";
    }
}

public sealed class CatalogGrain(IDatabase database) : Grain, ICatalog
{
    private readonly CatalogState _catalog = new(database);

    public Task<string> RunAsync(string operation) => _catalog.ExecuteAsync(operation);
}

internal sealed class CatalogActor : ReceiveActor
{
    public CatalogActor(IDatabase database)
    {
        var catalog = new CatalogState(database);
        ReceiveAsync<string>(async operation =>
        {
            var caller = Sender;
            caller.Tell(await catalog.ExecuteAsync(operation).ConfigureAwait(false));
        });
    }
}

internal static class Program
{
    public static async Task Main(string[] args)
    {
        using var redis = await ConnectionMultiplexer
            .ConnectAsync("store:6379")
            .ConfigureAwait(false);
        var database = redis.GetDatabase();
        if (args[0] == "replay")
        {
            Console.WriteLine(
                await CatalogState.CommitAsync(database, args[1], args[2]).ConfigureAwait(false)
            );
            return;
        }
        var address = IPAddress.Parse(args[1]);
        if (args[0] == "orleans")
        {
            var builder = Host.CreateApplicationBuilder();
            builder.Services.AddSingleton(database);
            builder.UseOrleans(silo =>
                silo.Configure<ClusterOptions>(options =>
                    {
                        options.ClusterId = "trial";
                        options.ServiceId = "catalog";
                    })
                    .ConfigureEndpoints(address, 11111, 30000)
                    .Configure<ClusterMembershipOptions>(options =>
                    {
                        options.ProbeTimeout = TimeSpan.FromSeconds(2);
                        options.NumMissedProbesLimit = 3;
                        options.NumVotesForDeathDeclaration = 2;
                        options.TableRefreshTimeout = TimeSpan.FromSeconds(3);
                    })
                    .UseRedisClustering("store:6379")
            );
            using var host = builder.Build();
            await host.StartAsync().ConfigureAwait(false);
            var grains = host.Services.GetRequiredService<IGrainFactory>();
            var catalog = grains.GetGrain<ICatalog>("eu");
            await ServeAsync(
                    database,
                    async operation =>
                        operation == "members"
                            ? (
                                await grains
                                    .GetGrain<IManagementGrain>(0)
                                    .GetHosts(true)
                                    .ConfigureAwait(false)
                            ).Count.ToString(System.Globalization.CultureInfo.InvariantCulture)
                            : await catalog.RunAsync(operation).ConfigureAwait(false)
                )
                .ConfigureAwait(false);
            return;
        }

        var config = ConfigurationFactory.ParseString(
            $$"""
            akka.actor.provider = cluster
            akka.remote.dot-netty.tcp.hostname = "{{address}}"
            akka.remote.dot-netty.tcp.port = 2552
            akka.cluster.seed-nodes = ["akka.tcp://trial@{{args[2]}}:2552"]
            akka.cluster.downing-provider-class = "Akka.Cluster.SBR.SplitBrainResolverProvider, Akka.Cluster"
            akka.cluster.split-brain-resolver.active-strategy = static-quorum
            akka.cluster.split-brain-resolver.static-quorum.quorum-size = 2
            akka.cluster.split-brain-resolver.stable-after = 5s
            akka.cluster.sharding.state-store-mode = ddata
            akka.cluster.sharding.distributed-data.durable.keys = []
            """
        );
        using var system = ActorSystem.Create("trial", config);
        var region = await ClusterSharding
            .Get(system)
            .StartAsync(
                "catalog",
                Props.Create(() => new CatalogActor(database)),
                ClusterShardingSettings.Create(system),
                HashCodeMessageExtractor.Create(
                    8,
                    message => message is string ? "eu" : null,
                    message => message
                )
            )
            .ConfigureAwait(false);
        await ServeAsync(
                database,
                operation =>
                    operation == "members"
                        ? Task.FromResult(
                            Cluster
                                .Get(system)
                                .State.Members.Count(member => member.Status == MemberStatus.Up)
                                .ToString(System.Globalization.CultureInfo.InvariantCulture)
                        )
                        : region.Ask<string>(operation, TimeSpan.FromSeconds(5))
            )
            .ConfigureAwait(false);
    }

    private static async Task ServeAsync(IDatabase database, Func<string, Task<string>> route)
    {
        var voter = Environment.GetEnvironmentVariable("NODE")!;
        var inbox = $"in:{voter}";
        var witness = await File.ReadAllTextAsync(
                Path.Combine(AppContext.BaseDirectory, "witness.lua")
            )
            .ConfigureAwait(false);
        while (true)
        {
            var request = await database.ListLeftPopAsync(inbox).ConfigureAwait(false);
            if (request.IsNull)
            {
                await Task.Delay(40).ConfigureAwait(false);
                continue;
            }
            var parts = request.ToString().Split('|', 2);
            string response;
            try
            {
                var reading = parts[1] == "read";
                var expected = reading
                    ? (await database.StringGetAsync("writer").ConfigureAwait(false)).ToString()
                    : "";
                response = await route(parts[1])
                    .WaitAsync(TimeSpan.FromSeconds(7))
                    .ConfigureAwait(false);
                if (reading)
                {
                    var candidate = response.Split('|', 2)[0];
                    await database
                        .ScriptEvaluateAsync(
                            witness,
                            ["writer", "witnesses", "retired"],
                            [expected, voter, candidate]
                        )
                        .ConfigureAwait(false);
                }
            }
            catch (Exception error)
            {
                response = $"error:{error.GetType().Name}";
            }
            await database
                .StringSetAsync($"out:{parts[0]}", response, TimeSpan.FromMinutes(5))
                .ConfigureAwait(false);
        }
    }
}
