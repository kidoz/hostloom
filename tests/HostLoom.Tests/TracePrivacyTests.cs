using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using HostLoom.Leadership;
using HostLoom.Locking;
using HostLoom.NodeLeasing;
using HostLoom.Pipelines;
using HostLoom.Pipelines.DependencyInjection;
using HostLoom.Transport.InMemory;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using NSubstitute;
using Xunit;

namespace HostLoom.Tests;

public sealed class TracePrivacyTests
{
    private const string Secret = "Password=trace-private-value;Token=private-token";

    [Theory]
    [InlineData("request", "hostloom handle request")]
    [InlineData("event", "hostloom handle event")]
    [InlineData("stream", "hostloom handle stream")]
    public async Task Messaging_failure_spans_do_not_export_exception_text(
        string path,
        string operation
    )
    {
        using var traces = new Traces();
        var builder = Host.CreateApplicationBuilder();
        builder
            .Services.AddHostLoom()
            .UseInMemory()
            .AddHandler<Ask, string, FailingHandler>("catalog")
            .AddSubscriber<Placed, FailingSubscriber>("orders", "audit")
            .AddStreamHandler<ListCatalog, string, FailingStream>("catalog");
        using var host = builder.Build();
        await host.StartAsync(TestContext.Current.CancellationToken);
        if (path == "request")
            await Assert.ThrowsAsync<RemoteRequestException>(async () =>
                await host
                    .Services.GetRequiredService<IRequestClient<Ask, string>>()
                    .GetResponseAsync(
                        "catalog",
                        new Ask(),
                        cancellationToken: TestContext.Current.CancellationToken
                    )
            );
        else if (path == "event")
            await host
                .Services.GetRequiredService<IPublishEndpoint>()
                .PublishAsync("orders", new Placed(), TestContext.Current.CancellationToken);
        else
            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            {
                await foreach (
                    var _ in host
                        .Services.GetRequiredService<IStreamRequestClient<ListCatalog, string>>()
                        .StreamAsync(
                            "catalog",
                            new ListCatalog(),
                            cancellationToken: TestContext.Current.CancellationToken
                        )
                ) { }
            });
        traces.AssertPrivateFailure(operation);
    }

    [Fact]
    public async Task Pipeline_run_and_filter_failures_do_not_export_exception_text()
    {
        using var traces = new Traces();
        var services = new ServiceCollection();
        services.AddPipeline<PipeContext>(
            "catalog",
            pipeline => pipeline.Stage("execute", stage => stage.AddFilter<FailingFilter>())
        );
        await using var provider = services.BuildServiceProvider();
        var runner = provider.GetRequiredKeyedService<IPipelineRunner<PipeContext>>("catalog");
        var error = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await runner.RunAsync(new PipeContext(TestContext.Current.CancellationToken))
        );
        Assert.Equal(Secret, error.Message);
        traces.AssertPrivateFailure("hostloom pipeline run");
        traces.AssertPrivateFailure("hostloom pipeline filter");
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Election_failures_do_not_export_provider_exception_text(bool nodeLeasing)
    {
        using var traces = new Traces();
        var clock = new TestClock();
        var locks = Substitute.For<IDistributedLock>();
        locks.IsCoordinated.Returns(true);
        locks
            .TryAcquireAsync(
                Arg.Any<string>(),
                Arg.Any<LockOptions>(),
                Arg.Any<CancellationToken>()
            )
            .Returns(_ =>
                ValueTask.FromException<ILockHandle?>(
                    new LockProviderUnavailableException(
                        Secret,
                        TimeSpan.Zero,
                        1,
                        LockFailureKind.Timeout,
                        new InvalidOperationException(Secret)
                    )
                )
            );
        if (nodeLeasing)
        {
            await using var elector = new NodeIdElector(
                "catalog",
                locks,
                new NodeLeasingOptions { PoolSize = 1 },
                clock
            );
            await elector.StartAsync(TestContext.Current.CancellationToken);
            await SchedulingTests.WaitUntilAsync(() => traces.HasFailure);
            await elector.StopAsync(TestContext.Current.CancellationToken);
        }
        else
        {
            await using var elector = new LeaderElector(
                "catalog",
                locks,
                new LeadershipOptions(),
                clock
            );
            await elector.StartAsync(TestContext.Current.CancellationToken);
            await SchedulingTests.WaitUntilAsync(() => traces.HasFailure);
            await elector.StopAsync(TestContext.Current.CancellationToken);
        }
        traces.AssertPrivateFailures();
    }

    private sealed class Traces : IDisposable
    {
        private readonly Activity _parent = new Activity("privacy-test").Start();
        private readonly ConcurrentQueue<Activity> _stopped = new();
        private readonly ActivityListener _listener;

        public Traces()
        {
            _listener = new ActivityListener
            {
                ShouldListenTo = source =>
                    source.Name
                        is "HostLoom"
                            or "HostLoom.Pipelines"
                            or "HostLoom.Leadership"
                            or "HostLoom.NodeLeasing",
                Sample = (ref ActivityCreationOptions<ActivityContext> _) =>
                    ActivitySamplingResult.AllDataAndRecorded,
                ActivityStopped = activity =>
                {
                    if (activity.TraceId == _parent.TraceId)
                        _stopped.Enqueue(activity);
                },
            };
            ActivitySource.AddActivityListener(_listener);
        }

        public bool HasFailure =>
            _stopped.Any(activity => activity.Status == ActivityStatusCode.Error);

        public void AssertPrivateFailure(string operation)
        {
            var failure = Assert.Single(
                _stopped,
                activity =>
                    activity.OperationName == operation
                    && activity.Status == ActivityStatusCode.Error
            );
            Assert.True(string.IsNullOrEmpty(failure.StatusDescription));
            AssertPrivateFailures();
        }

        public void AssertPrivateFailures()
        {
            Assert.True(HasFailure);
            Assert.All(
                _stopped,
                activity =>
                {
                    Assert.DoesNotContain(
                        Secret,
                        activity.StatusDescription ?? "",
                        StringComparison.Ordinal
                    );
                    Assert.All(
                        activity.TagObjects,
                        tag =>
                            Assert.DoesNotContain(
                                Secret,
                                tag.Value?.ToString() ?? "",
                                StringComparison.Ordinal
                            )
                    );
                    Assert.All(
                        activity.Events,
                        evt =>
                            Assert.All(
                                evt.Tags,
                                tag =>
                                    Assert.DoesNotContain(
                                        Secret,
                                        tag.Value?.ToString() ?? "",
                                        StringComparison.Ordinal
                                    )
                            )
                    );
                }
            );
        }

        public void Dispose()
        {
            _listener.Dispose();
            _parent.Dispose();
        }
    }

    public sealed record Ask : IRequest<string>;

    public sealed record Placed : IEvent;

    public sealed record ListCatalog : IStreamRequest<string>;

    public sealed class FailingHandler : IRequestHandler<Ask, string>
    {
        public ValueTask<string> HandleAsync(Ask request, CancellationToken cancellationToken) =>
            throw new InvalidOperationException(Secret);
    }

    public sealed class FailingSubscriber : IEventHandler<Placed>
    {
        public ValueTask HandleAsync(Placed @event, CancellationToken cancellationToken) =>
            throw new InvalidOperationException(Secret);
    }

    public sealed class FailingStream : IStreamRequestHandler<ListCatalog, string>
    {
        public async IAsyncEnumerable<string> HandleAsync(
            ListCatalog request,
            [EnumeratorCancellation] CancellationToken cancellationToken
        )
        {
            await Task.CompletedTask;
            yield return Throw();
        }

        private static string Throw() => throw new InvalidOperationException(Secret);
    }

    public sealed class FailingFilter : IFilter<PipeContext>
    {
        public ValueTask SendAsync(PipeContext context, IPipe<PipeContext> next) =>
            throw new InvalidOperationException(Secret);

        public void Probe(IProbeContext context) { }
    }
}
