using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Diagnostics.Metrics;
using System.Globalization;
using System.Runtime;
using System.Security.Cryptography;
using System.Text.Json;
using HostLoom.DataGrid;
using HostLoom.DataGrid.Client;
using HostLoom.DataGrid.ProcessProbe;
using Xunit;
using Cluster = HostLoom.Tests.DataGridProcessTests.ProcessCluster;
using GridClient = HostLoom.DataGrid.Client.DataGridClient;

namespace HostLoom.Tests;

[Collection(nameof(DataGridDiagnosticsTests))]
public sealed class DataGridFeasibilityTests
{
    [Fact(Timeout = 900000)]
    public async Task OwnedThreeProcessFeasibilityCase()
    {
        Assert.SkipUnless(
            Environment.GetEnvironmentVariable("HOSTLOOM_GRID_FEASIBILITY") == "1",
            "Requires explicit owned feasibility runner."
        );
        Assert.True(OperatingSystem.IsLinux() && File.Exists("/.dockerenv"));
        var owner = Environment.GetEnvironmentVariable("HOSTLOOM_GRID_FEASIBILITY_OWNER");
        Assert.True(Guid.TryParseExact(owner, "N", out _));
        Assert.Equal(
            owner,
            await File.ReadAllTextAsync(
                "/work/FEASIBILITY_OWNER",
                TestContext.Current.CancellationToken
            )
        );
        var spec = JsonSerializer.Deserialize<DataGridFeasibilityCase>(
            Environment.GetEnvironmentVariable("HOSTLOOM_GRID_FEASIBILITY_CASE")!,
            DataGridFeasibilityCase.Json
        )!;
        spec.Validate();
        Directory.CreateDirectory("/work/feasibility");
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(
            TestContext.Current.CancellationToken
        );
        deadline.CancelAfter(TimeSpan.FromSeconds(840));
        var report = new Dictionary<string, object?>
        {
            ["schema_version"] = 1,
            ["case"] = spec,
            ["is_p0"] = spec.IsP0,
            ["status"] = "incomplete",
            ["acceptance"] = "no_numerical_targets",
            ["load_model"] = "closed_loop_one_outstanding_per_worker",
            ["key_bytes"] = 32,
            ["value_bytes"] = 1024,
            ["read_write_schedule"] = "four_reads_then_one_write_per_worker",
            ["expiry"] = "even_keys_never_odd_keys_relative_60_seconds_no_refill",
            ["principals"] = new { Workload = 1, Setup = 1 },
            ["limits"] = new
            {
                MapKeys = 100000,
                MapBytes = 128L * 1024 * 1024,
                GroupKeys = 100000,
                GroupBytes = 256L * 1024 * 1024,
                PrincipalSessions = 64,
                PrincipalOutstanding = 8,
                ClientCallsPerInstance = 8,
                ClientStartsPerSecond = 8,
                SdkStartsPerSecond = 2,
                SdkAttempts = 3,
                SdkDeadlineSeconds = 5,
            },
            ["runtime"] = new
            {
                Framework = System.Runtime.InteropServices.RuntimeInformation.FrameworkDescription,
                ServerGc = GCSettings.IsServerGC,
                ProcessorCount = Environment.ProcessorCount,
            },
            ["instrumentation"] =
                "one-second process/io/directory snapshots; raw call serialization after each phase",
        };
        var token = deadline.Token;
        var workers = new List<Worker>();
        var clock = Stopwatch.StartNew();
        await using var cluster = new Cluster(feasibility: true);
        using var monitorStop = new CancellationTokenSource();
        var phase = "startup";
        string? resourceFailure = null;
        var monitor = Monitor(
            cluster,
            () => phase,
            reason =>
            {
                if (Interlocked.CompareExchange(ref resourceFailure, reason, null) is null)
                    deadline.Cancel();
            },
            monitorStop.Token
        );
        try
        {
            await cluster.StartAll(token);
            var leader = await cluster.Leader(token);
            report["startup_to_leader_role_seconds"] = clock.Elapsed.TotalSeconds;
            phase = "population";
            var setupClients = new List<GridClient>();
            try
            {
                using var population = CancellationTokenSource.CreateLinkedTokenSource(token);
                population.CancelAfter(TimeSpan.FromSeconds(300));
                var sessions = new List<long>();
                for (var lane = 0; lane < 4; lane++)
                {
                    var setup = cluster.Client(leader, setup: true);
                    setupClients.Add(setup);
                    sessions.Add(await Open(setup, population.Token));
                }
                report["startup_to_sessions_seconds"] = clock.Elapsed.TotalSeconds;
                var sequences = new ulong[] { 1, 1, 1, 1 };
                var stored = 0;
                var populationStarted = Stopwatch.GetTimestamp();
                double ttlPopulationSeconds = 0;
                for (var parity = 0; parity < 2; parity++)
                {
                    var stageAt = Stopwatch.GetTimestamp();
                    await Task.WhenAll(
                        Enumerable
                            .Range(0, 4)
                            .Select(async lane =>
                            {
                                for (
                                    var first = lane * 128;
                                    first < spec.KeyCount / 2;
                                    first += 4 * 128
                                )
                                {
                                    var indices = Enumerable
                                        .Range(first, Math.Min(128, spec.KeyCount / 2 - first))
                                        .Select(i => parity + 2 * i)
                                        .ToArray();
                                    var request = DataGridFeasibilityCase.Put(
                                        sessions[lane],
                                        sequences[lane],
                                        indices,
                                        spec.Seed,
                                        -1 - lane,
                                        parity == 1
                                    );
                                    var result = await Resolve(
                                        setupClients[lane],
                                        request,
                                        population.Token
                                    );
                                    if (
                                        result.Result!.Status != DataGridApplyStatus.Completed
                                        || Enumerable
                                            .Range(0, result.Result.ItemCount)
                                            .Any(i =>
                                                result.Result.GetItem(i).Status
                                                != DataGridMutationStatus.Stored
                                            )
                                    )
                                        throw new Infeasible(
                                            "Dataset population rejected by effective capacity/session policy."
                                        );
                                    Interlocked.Add(ref stored, indices.Length);
                                    sequences[lane]++;
                                }
                            })
                    );
                    if (parity == 1)
                        ttlPopulationSeconds = Stopwatch.GetElapsedTime(stageAt).TotalSeconds;
                }
                report["population"] = new
                {
                    AcknowledgedKeys = stored,
                    NeverKeys = stored / 2,
                    TtlKeys = stored / 2,
                    Seconds = Stopwatch.GetElapsedTime(populationStarted).TotalSeconds,
                    TtlPopulationSeconds = ttlPopulationSeconds,
                    SetupBatch = 128,
                    SetupClients = 4,
                    CurrentLiveKeys = (int?)null,
                    Caveat = "TTL starts on each accepted write; acknowledged keys are not an assertion of simultaneous live population.",
                };
            }
            finally
            {
                foreach (var setup in setupClients)
                    await setup.DisposeAsync();
            }
            for (var i = 0; i < spec.Concurrency; i++)
            {
                var client = cluster.Client(leader);
                try
                {
                    workers.Add(new(i, client, await Open(client, token), spec));
                }
                catch
                {
                    await client.DisposeAsync();
                    throw;
                }
            }
            phase = "warmup";
            report["warmup"] = await RunPhase(workers, spec.WarmupSeconds, "warmup", token);
            phase = "sample";
            report["sample"] = await RunPhase(workers, spec.SampleSeconds, "sample", token);
            phase = "settle";
            using (var settle = CancellationTokenSource.CreateLinkedTokenSource(token))
            {
                settle.CancelAfter(TimeSpan.FromSeconds(45));
                foreach (var worker in workers)
                    if (worker.Pending is { } pending)
                    {
                        _ = await Resolve(worker.Client, pending, settle.Token);
                        worker.Pending = null;
                        worker.Sequence++;
                    }
            }
            // All last unknown identities are resolved before a canary can be overwritten by
            // delayed earlier work. The canary is outside every timed warmup/sample window.
            phase = "recovery";
            var canary = workers[^1];
            var requestCanary = DataGridFeasibilityCase.Put(
                canary.Session,
                canary.Sequence,
                [0],
                spec.Seed,
                canary.Id,
                ttl: false
            );
            var original = (await Resolve(canary.Client, requestCanary, token)).Result!.GetItem(0);
            await cluster.AppliedEverywhere(original.Version.LogIndex, token);
            leader = await cluster.Leader(token);
            var failureAt = Stopwatch.GetTimestamp();
            await cluster.Kill(leader);
            await using (var survivor = cluster.Client((leader + 1) % 3))
            {
                await StrongCanary(survivor, original.Version, token);
                report["leader_loss_to_strong_read_seconds"] = Stopwatch
                    .GetElapsedTime(failureAt)
                    .TotalSeconds;
                var replay = await Resolve(survivor, requestCanary, token);
                Assert.Equal(original, replay.Result!.GetItem(0));
            }
            await cluster.Start(leader, token);
            await cluster.AppliedEverywhere(original.Version.LogIndex, token);
            for (var i = 0; i < 3; i++)
                await cluster.Kill(i);
            var restartAt = Stopwatch.GetTimestamp();
            await cluster.StartAll(token);
            await using (var reopened = cluster.Client(await cluster.Leader(token)))
            {
                var replay = await Resolve(reopened, requestCanary, token);
                Assert.Equal(original, replay.Result!.GetItem(0));
                await StrongCanary(reopened, original.Version, token);
                report["whole_group_restart_to_retained_replay_seconds"] = Stopwatch
                    .GetElapsedTime(restartAt)
                    .TotalSeconds;
            }
            report["recovery_verified"] = true;
            report["status"] = "completed";
        }
        catch (Infeasible exception)
        {
            report["status"] = "infeasible";
            report["reason"] = exception.Message;
        }
        catch (OperationCanceledException)
            when (!TestContext.Current.CancellationToken.IsCancellationRequested)
        {
            var resourceReason = Volatile.Read(ref resourceFailure);
            report["status"] = resourceReason is null ? "incomplete_budget" : "infeasible";
            report["reason"] =
                resourceReason ?? "A declared setup/settle/case deadline prevented completion.";
        }
        catch (Exception exception)
        {
            report["status"] = "failed";
            report["error"] = exception.ToString();
            throw;
        }
        finally
        {
            phase = "cleanup";
            try
            {
                await monitorStop.CancelAsync();
                await monitor;
                foreach (var worker in workers)
                    await worker.Client.DisposeAsync();
            }
            finally
            {
                report["elapsed_seconds"] = clock.Elapsed.TotalSeconds;
                report["unresolved_mutations"] = workers.Count(w => w.Pending is not null);
                await File.WriteAllTextAsync(
                    "/work/feasibility/case.json",
                    JsonSerializer.Serialize(report, DataGridFeasibilityCase.Json),
                    CancellationToken.None
                );
            }
        }
    }

    private static async Task<long> Open(GridClient client, CancellationToken token)
    {
        var result = await Resolve(
            client,
            DataGridClientRequest.OpenSession(
                Guid.NewGuid(),
                DataGridTimestamp.FromUtc(DateTimeOffset.UtcNow.AddSeconds(4))
            ),
            token
        );
        return result.Result!.SessionNumber;
    }

    private static async Task<DataGridClientReply> Resolve(
        GridClient client,
        DataGridClientRequest request,
        CancellationToken token
    )
    {
        using var bound = CancellationTokenSource.CreateLinkedTokenSource(token);
        bound.CancelAfter(TimeSpan.FromSeconds(45));
        using var pacing = new PeriodicTimer(TimeSpan.FromMilliseconds(150));
        do
        {
            var result = await client.ExecuteAsync(request, bound.Token);
            if (result.Reply.Failure is null)
                return result.Reply;
            if (
                result.Reply.Failure.Code
                is not (
                    DataGridClientFailureCode.CapacityExceeded
                    or DataGridClientFailureCode.QuorumUnavailable
                    or DataGridClientFailureCode.OutcomeUnknown
                    or DataGridClientFailureCode.Redirect
                )
            )
                throw new Infeasible(
                    "Immutable identity could not resolve: " + result.Reply.Failure.Code
                );
        } while (await pacing.WaitForNextTickAsync(bound.Token));
        throw new Infeasible("Unresolved immutable request.");
    }

    private static async Task StrongCanary(
        GridClient client,
        DataGridEntryVersion version,
        CancellationToken token
    )
    {
        var read = await Resolve(
            client,
            DataGridClientRequest.Read(ProbeConfiguration.Map, [DataGridFeasibilityCase.Key(0)]),
            token
        );
        Assert.Equal(version, read.Read!.GetItem(0)!.Version);
        DataGridFeasibilityCase.ValidateValue(read.Read.GetItem(0)!.GetValue());
    }

    private sealed class Worker(
        int id,
        GridClient client,
        long session,
        DataGridFeasibilityCase spec
    )
    {
        internal int Id { get; } = id;
        internal GridClient Client { get; } = client;
        internal long Session { get; } = session;
        internal ulong Sequence { get; set; } = 1;
        internal DataGridClientRequest? Pending { get; set; }
        internal long LogicalAt { get; set; }
        internal string Origin { get; set; } = "";
        internal long Ordinal { get; set; } = id;
        internal Random Random { get; } = new(unchecked(spec.Seed * 397 + id));
        internal bool Terminal { get; set; }
        internal DataGridFeasibilityCase Spec { get; } = spec;
    }

    [SuppressMessage(
        "Security",
        "CA5394:Do not use insecure randomness",
        Justification = "Seeded key selection is reproducible load generation; credentials still use the cryptographic fixture RNG."
    )]
    private static async Task<object> RunPhase(
        List<Worker> workers,
        int seconds,
        string phase,
        CancellationToken token
    )
    {
        long sdkTimeouts = 0;
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, observer) =>
        {
            if (
                instrument.Meter.Name == DataGridClientDiagnostics.MeterName
                && instrument.Name == "hostloom.datagrid.client.timeouts"
            )
                observer.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>(
            (_, value, _, _) => Interlocked.Add(ref sdkTimeouts, value)
        );
        listener.Start();
        var began = Stopwatch.GetTimestamp();
        var end = began + (long)seconds * Stopwatch.Frequency;
        var reservedInvocations = 0;
        using var duration = CancellationTokenSource.CreateLinkedTokenSource(token);
        if (seconds == 0)
            await duration.CancelAsync();
        else
            duration.CancelAfter(TimeSpan.FromSeconds(seconds));
        var outcomes = await Task.WhenAll(
            workers.Select(async worker =>
            {
                var rows = new List<DataGridFeasibilityCall>();
                while (
                    !duration.IsCancellationRequested
                    && Stopwatch.GetTimestamp() < end
                    && !worker.Terminal
                )
                {
                    if (Interlocked.Increment(ref reservedInvocations) > 100000)
                        throw new Infeasible(
                            "Raw invocation sample bound reached; no observations silently dropped."
                        );
                    var fresh = worker.Pending is null;
                    var mutation = !fresh || worker.Ordinal % 5 == 4;
                    DataGridClientRequest request;
                    if (!fresh)
                        request = worker.Pending!;
                    else
                    {
                        var parity = mutation ? (int)(worker.Sequence % 2) : worker.Random.Next(2);
                        var first = worker.Random.Next(worker.Spec.KeyCount / 2);
                        var indices = Enumerable
                            .Range(0, worker.Spec.BatchSize)
                            .Select(i => parity + 2 * ((first + i) % (worker.Spec.KeyCount / 2)))
                            .ToArray();
                        request = mutation
                            ? DataGridFeasibilityCase.Put(
                                worker.Session,
                                worker.Sequence,
                                indices,
                                worker.Spec.Seed,
                                worker.Id,
                                parity == 1
                            )
                            : DataGridClientRequest.Read(
                                ProbeConfiguration.Map,
                                indices.Select(DataGridFeasibilityCase.Key).ToArray()
                            );
                        worker.Ordinal++;
                        if (mutation)
                        {
                            worker.Pending = request;
                            worker.LogicalAt = Stopwatch.GetTimestamp();
                            worker.Origin = phase;
                        }
                    }
                    var digest = mutation
                        ? Convert.ToHexString(SHA256.HashData(request.Mutation!.Encode()))
                        : null;
                    var at = Stopwatch.GetTimestamp();
                    DataGridClientOutcome? reply = null;
                    var result = "cancelled";
                    try
                    {
                        reply = await worker.Client.ExecuteAsync(request, duration.Token);
                        result = reply.Reply.Failure?.Code.ToString() ?? "completed";
                        if (
                            reply.Reply.Failure?.Code
                            is DataGridClientFailureCode.RequestIdentityMismatch
                                or DataGridClientFailureCode.RequestRetired
                                or DataGridClientFailureCode.InvalidRequest
                        )
                            throw new InvalidDataException(
                                "Feasibility request sequencing/identity failed: " + result
                            );
                    }
                    catch (OperationCanceledException) when (duration.IsCancellationRequested) { }
                    var finished = Stopwatch.GetTimestamp();
                    var hits = 0;
                    var failedItems = 0;
                    if (reply?.Reply.Failure is null && reply is not null)
                    {
                        if (mutation)
                        {
                            Assert.Equal(DataGridApplyStatus.Completed, reply.Reply.Result!.Status);
                            for (var i = 0; i < reply.Reply.Result.ItemCount; i++)
                                if (
                                    reply.Reply.Result.GetItem(i).Status
                                    != DataGridMutationStatus.Stored
                                )
                                    failedItems++;
                        }
                        else
                            for (var i = 0; i < reply.Reply.Read!.Count; i++)
                                if (reply.Reply.Read!.GetItem(i) is { } item)
                                {
                                    hits++;
                                    DataGridFeasibilityCase.ValidateValue(item.GetValue());
                                }
                    }
                    var logicalDone = mutation && reply is not null && reply.Reply.Failure is null;
                    rows.Add(
                        new(
                            worker.Id,
                            mutation ? "write" : "read",
                            mutation ? worker.Sequence : 0,
                            digest,
                            fresh,
                            logicalDone,
                            worker.Origin == phase,
                            (at - began) * 1e6 / Stopwatch.Frequency,
                            (finished - at) * 1e6 / Stopwatch.Frequency,
                            logicalDone
                                ? (finished - worker.LogicalAt) * 1e6 / Stopwatch.Frequency
                                : null,
                            finished <= end,
                            result,
                            reply?.MayHaveBeenAdmitted ?? mutation,
                            reply?.Attempts.Count ?? 0,
                            worker.Spec.BatchSize,
                            hits,
                            failedItems
                        )
                    );
                    if (logicalDone)
                    {
                        worker.Pending = null;
                        worker.Sequence++;
                    }
                    if (
                        reply?.Reply.Failure?.Code
                        is DataGridClientFailureCode.SessionExpired
                            or DataGridClientFailureCode.RequestRetired
                            or DataGridClientFailureCode.RequestIdentityMismatch
                            or DataGridClientFailureCode.InvalidRequest
                    )
                        worker.Terminal = true;
                }
                return rows;
            })
        );
        var drainSeconds = Stopwatch.GetElapsedTime(began).TotalSeconds;
        var calls = outcomes.SelectMany(rows => rows).ToArray();
        await using (var file = new StreamWriter("/work/feasibility/" + phase + "-calls.jsonl"))
            foreach (var call in calls)
                await file.WriteLineAsync(
                    JsonSerializer
                        .Serialize(call, DataGridFeasibilityCase.Json)
                        .Replace("\n", "", StringComparison.Ordinal)
                );
        var inWindow = calls.Where(c => c.WithinWindow && c.Outcome != "cancelled").ToArray();
        var success = inWindow
            .Where(c => c.Outcome == "completed" && c.ItemFailures == 0)
            .ToArray();
        var logical = inWindow
            .Where(c => c.LogicalCompletedHere && c.OriginatedHere)
            .Select(c => c.LogicalDurationUs!.Value)
            .ToArray();
        var unknown = calls.Count(c => c.Outcome == "OutcomeUnknown");
        var refused = calls.Count(c =>
            c.Outcome is not ("completed" or "cancelled") && !c.MayHaveBeenAdmitted
        );
        var errors = calls.Count(c => c.Outcome != "completed" || c.ItemFailures != 0);
        var timeouts = Interlocked.Read(ref sdkTimeouts);
        double Rate(long count) => calls.Length == 0 ? 0 : count / (double)calls.Length;
        return new
        {
            RequestedSeconds = seconds,
            IncludingDrainSeconds = drainSeconds,
            Invocations = calls.Length,
            OfferedLogicalReads = calls.Count(c => c.NewLogical && c.Operation == "read"),
            OfferedLogicalWrites = calls.Count(c => c.NewLogical && c.Operation == "write"),
            CompletedWithinWindow = inWindow.Length,
            SuccessfulWithinWindow = success.Length,
            LateOrCensored = calls.Length - inWindow.Length,
            ReadHits = calls.Sum(c => c.Hits),
            ItemFailures = calls.Sum(c => c.ItemFailures),
            Unknown = unknown,
            UnknownRate = Rate(unknown),
            Refused = refused,
            RefusalRate = Rate(refused),
            Errors = errors,
            ErrorRate = Rate(errors),
            SdkTimeouts = timeouts,
            SdkTimeoutRate = Rate(timeouts),
            RateDenominator = "all started SDK invocations including late/cancelled; refusal excludes possibly admitted calls; SDK timeout is the dedicated deadline counter",
            TerminalWorkers = workers.Count(w => w.Terminal),
            UnresolvedWrites = workers.Count(w => w.Pending is not null),
            RequestThroughput = seconds == 0 ? 0 : success.Length / (double)seconds,
            ItemThroughput = seconds == 0 ? 0 : success.Sum(c => c.Items) / (double)seconds,
            Outcomes = calls.GroupBy(c => c.Outcome).ToDictionary(g => g.Key, g => g.Count()),
            SuccessfulCallUs = Quantiles(success.Select(c => c.DurationUs)),
            AllCompletedCallUs = Quantiles(inWindow.Select(c => c.DurationUs)),
            LogicalWriteUs = Quantiles(logical),
            PercentileDefinition = "nearest_rank_individual_sdk_invocations_or_resolved_logical_writes; no per-item average; late/censored excluded and counted",
        };
    }

    private static object Quantiles(IEnumerable<double> values)
    {
        var copy = values.ToArray();
        return new
        {
            Count = copy.Length,
            P50 = DataGridFeasibilityCase.Percentile(copy, .50),
            P95 = DataGridFeasibilityCase.Percentile(copy, .95),
            P99 = DataGridFeasibilityCase.Percentile(copy, .99),
        };
    }

    private static async Task Monitor(
        Cluster cluster,
        Func<string> phase,
        Action<string> abort,
        CancellationToken token
    )
    {
        await using var writer = new StreamWriter("/work/feasibility/resources.jsonl");
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(1));
        var began = Stopwatch.GetTimestamp();
        long? initialOomKills = null;
        try
        {
            do
            {
                var members = new List<object>();
                for (var i = -1; i < 3; i++)
                {
                    try
                    {
                        var node = i < 0 ? null : cluster.Nodes[i];
                        var pid = i < 0 ? Environment.ProcessId : node?.ProcessId;
                        if (pid is null)
                        {
                            members.Add(new { Member = i, State = "stopped" });
                            continue;
                        }
                        using var process = Process.GetProcessById(pid.Value);
                        process.Refresh();
                        var io = (await File.ReadAllLinesAsync($"/proc/{pid.Value}/io", token))
                            .Select(line => line.Split(':', 2))
                            .ToDictionary(
                                pair => pair[0],
                                pair => long.Parse(pair[1], CultureInfo.InvariantCulture)
                            );
                        var files =
                            i < 0
                                ? []
                                : Directory.GetFiles(
                                    Path.Combine(cluster.DirectoryPath, $"node-{i}")
                                );
                        members.Add(
                            new
                            {
                                Member = i,
                                Pid = pid,
                                RssBytes = process.WorkingSet64,
                                CpuSeconds = process.TotalProcessorTime.TotalSeconds,
                                Managed = i < 0
                                    ? ProbeMemorySnapshot.Capture()
                                    : JsonSerializer.Deserialize<ProbeMemorySnapshot>(
                                        await node!.Command("memory", token)
                                    ),
                                Io = io,
                                OwnedFileBytes = files.Sum(path => new FileInfo(path).Length),
                                Files = files.Length,
                            }
                        );
                    }
                    catch (Exception error)
                        when (error
                                is IOException
                                    or InvalidOperationException
                                    or ArgumentException
                                    or System.ComponentModel.Win32Exception
                        )
                    {
                        members.Add(
                            new
                            {
                                Member = i,
                                State = "unavailable",
                                Error = error.GetType().Name,
                            }
                        );
                    }
                }
                object cgroup;
                string? resourceFailure = null;
                try
                {
                    var memoryEvents = await File.ReadAllTextAsync(
                        "/sys/fs/cgroup/memory.events",
                        token
                    );
                    var kills = long.Parse(
                        memoryEvents
                            .Split('\n')
                            .Single(line => line.StartsWith("oom_kill ", StringComparison.Ordinal))
                            .AsSpan(9),
                        CultureInfo.InvariantCulture
                    );
                    initialOomKills ??= kills;
                    if (kills > initialOomKills)
                        resourceFailure =
                            "Cgroup OOM kills increased during the case; the declared two-GiB fixture cannot complete this workload as configured.";
                    cgroup = new
                    {
                        MemoryCurrentBytes = long.Parse(
                            await File.ReadAllTextAsync("/sys/fs/cgroup/memory.current", token),
                            CultureInfo.InvariantCulture
                        ),
                        MemoryPeakBytes = long.Parse(
                            await File.ReadAllTextAsync("/sys/fs/cgroup/memory.peak", token),
                            CultureInfo.InvariantCulture
                        ),
                        MemoryEvents = memoryEvents,
                        MemoryStat = await File.ReadAllTextAsync(
                            "/sys/fs/cgroup/memory.stat",
                            token
                        ),
                    };
                }
                catch (IOException error)
                {
                    cgroup = new { State = "unavailable", Error = error.GetType().Name };
                }
                await writer.WriteLineAsync(
                    JsonSerializer
                        .Serialize(
                            new
                            {
                                Phase = phase(),
                                Seconds = Stopwatch.GetElapsedTime(began).TotalSeconds,
                                Members = members,
                                Cgroup = cgroup,
                            },
                            DataGridFeasibilityCase.Json
                        )
                        .Replace("\n", "", StringComparison.Ordinal)
                );
                await writer.FlushAsync(token);
                if (resourceFailure is not null)
                    abort(resourceFailure);
            } while (await timer.WaitForNextTickAsync(token));
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested) { }
    }

    private sealed class Infeasible(string message) : Exception(message);
}
