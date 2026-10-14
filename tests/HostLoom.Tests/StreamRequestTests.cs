using System.Runtime.CompilerServices;
using HostLoom.Transport.InMemory;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace HostLoom.Tests;

public sealed class StreamRequestTests
{
    [Fact]
    public async Task Stream_yields_items_in_order_beside_a_request_on_the_same_endpoint()
    {
        var builder = Host.CreateApplicationBuilder();
        builder
            .Services.AddHostLoom()
            .UseInMemory()
            .AddHandler<Ping, string, PingHandler>("catalog")
            .AddStreamHandler<ListCatalog, string, ListingCatalogHandler>("catalog");

        using var host = builder.Build();
        await host.StartAsync(TestContext.Current.CancellationToken);
        var token = TestContext.Current.CancellationToken;
        var pages = host.Services.GetRequiredService<IStreamRequestClient<ListCatalog, string>>();
        var ping = host.Services.GetRequiredService<IRequestClient<Ping, string>>();

        var items = new List<string>();
        await foreach (
            var item in pages.StreamAsync(
                "catalog",
                new ListCatalog("eu"),
                cancellationToken: token
            )
        )
        {
            items.Add(item);
        }

        Assert.Equal(["books", "music"], items);
        Assert.Equal(
            "eu",
            await ping.GetResponseAsync("catalog", new Ping("eu"), cancellationToken: token)
        );
    }

    [Fact]
    public async Task Each_enumeration_gets_its_own_scope_and_disposes_it()
    {
        var log = new List<string>();
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(log);
        builder.Services.AddScoped<CatalogScope>();
        builder
            .Services.AddHostLoom()
            .UseInMemory()
            .AddStreamHandler<ListCatalog, string, ScopedCatalogHandler>("catalog");

        using var host = builder.Build();
        await host.StartAsync(TestContext.Current.CancellationToken);
        var client = host.Services.GetRequiredService<IStreamRequestClient<ListCatalog, string>>();
        var token = TestContext.Current.CancellationToken;

        var first = new List<string>();
        await foreach (
            var id in client.StreamAsync("catalog", new ListCatalog("eu"), cancellationToken: token)
        )
        {
            first.Add(id);
        }

        var second = new List<string>();
        await foreach (
            var id in client.StreamAsync("catalog", new ListCatalog("eu"), cancellationToken: token)
        )
        {
            second.Add(id);
        }

        Assert.Equal([first[0], first[0]], first);
        Assert.Equal([second[0], second[0]], second);
        Assert.NotEqual(first[0], second[0]);
        Assert.Equal(
            [
                $"created:{first[0]}",
                $"disposed:{first[0]}",
                $"created:{second[0]}",
                $"disposed:{second[0]}",
            ],
            log
        );
    }

    [Fact]
    public async Task Handler_exception_reaches_the_caller_unchanged()
    {
        var builder = Host.CreateApplicationBuilder();
        builder
            .Services.AddHostLoom()
            .UseInMemory()
            .AddStreamHandler<ListCatalog, string, FailingCatalogHandler>("catalog");

        using var host = builder.Build();
        await host.StartAsync(TestContext.Current.CancellationToken);
        var client = host.Services.GetRequiredService<IStreamRequestClient<ListCatalog, string>>();

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await foreach (
                var _ in client.StreamAsync(
                    "catalog",
                    new ListCatalog("eu"),
                    cancellationToken: TestContext.Current.CancellationToken
                )
            ) { }
        });

        Assert.Equal("catalog unavailable", exception.Message);
    }

    [Fact]
    public async Task A_stream_for_an_unregistered_type_names_the_endpoint()
    {
        var builder = Host.CreateApplicationBuilder();
        builder
            .Services.AddHostLoom()
            .UseInMemory()
            .AddStreamHandler<ListCatalog, string, ListingCatalogHandler>("catalog")
            .AddStreamClient<OtherPages, string>();

        using var host = builder.Build();
        await host.StartAsync(TestContext.Current.CancellationToken);
        var client = host.Services.GetRequiredService<IStreamRequestClient<OtherPages, string>>();

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await foreach (
                var _ in client.StreamAsync(
                    "catalog",
                    new OtherPages("eu"),
                    cancellationToken: TestContext.Current.CancellationToken
                )
            ) { }
        });

        Assert.Contains("catalog", exception.Message, StringComparison.Ordinal);
        Assert.Contains(nameof(OtherPages), exception.Message, StringComparison.Ordinal);
    }

    [Fact(Timeout = 30_000)]
    public async Task Caller_cancellation_cancels_the_handler()
    {
        var cancelled = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(cancelled);
        builder
            .Services.AddHostLoom()
            .UseInMemory()
            .AddStreamHandler<ListCatalog, string, HoldingCatalogHandler>("catalog");

        using var host = builder.Build();
        await host.StartAsync(TestContext.Current.CancellationToken);
        var client = host.Services.GetRequiredService<IStreamRequestClient<ListCatalog, string>>();
        using var caller = new CancellationTokenSource();
        await using var enumerator = client
            .StreamAsync("catalog", new ListCatalog("eu"), cancellationToken: caller.Token)
            .GetAsyncEnumerator(caller.Token);

        Assert.True(await enumerator.MoveNextAsync());
        Assert.Equal("books", enumerator.Current);
        await caller.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await enumerator.MoveNextAsync()
        );
        await cancelled.Task.WaitAsync(TestContext.Current.CancellationToken);
    }

    [Fact(Timeout = 30_000)]
    public async Task A_stream_with_no_listener_times_out_on_the_transport_clock()
    {
        var token = TestContext.Current.CancellationToken;
        var clock = new TestClock();
        await using var broker = new InMemoryRequestBroker(logger: null, clock);
        var budget = TimeSpan.FromMinutes(5);
        await using var enumerator = broker
            .StreamAsync("catalog", new ListCatalog("eu"), budget, token)
            .GetAsyncEnumerator(token);
        var pending = enumerator.MoveNextAsync().AsTask();

        await SchedulingTests.WaitUntilAsync(() => clock.PendingTimers == 1);
        clock.Advance(budget - TimeSpan.FromSeconds(1));
        Assert.False(pending.IsCompleted);
        clock.Advance(TimeSpan.FromSeconds(1));

        var timeout = await Assert.ThrowsAsync<RequestTimeoutException>(() =>
            pending.WaitAsync(TimeSpan.FromSeconds(5), token)
        );
        Assert.Equal("catalog", timeout.Address.Value);
        Assert.Equal(budget, timeout.Timeout);
    }

    [Fact(Timeout = 30_000)]
    public async Task Disposing_the_transport_ends_an_open_stream()
    {
        var token = TestContext.Current.CancellationToken;
        var broker = new InMemoryRequestBroker();
        await broker.ListenAsync(
            "catalog",
            static (request, handlerToken) => Hold(handlerToken),
            token
        );
        await using var enumerator = broker
            .StreamAsync("catalog", new ListCatalog("eu"), TimeSpan.FromSeconds(5), token)
            .GetAsyncEnumerator(token);

        Assert.True(await enumerator.MoveNextAsync());
        Assert.Equal("books", enumerator.Current);
        await broker.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(async () =>
            await enumerator.MoveNextAsync()
        );
    }

    [Fact]
    public async Task A_transport_without_streaming_fails_startup_and_the_client_call()
    {
        var token = TestContext.Current.CancellationToken;
        var hosted = Host.CreateApplicationBuilder();
        hosted
            .Services.AddHostLoom()
            .UseTransport<RequestOnlyBroker>()
            .AddStreamHandler<ListCatalog, string, ListingCatalogHandler>("catalog");
        using var host = hosted.Build();

        var startup = await Assert.ThrowsAsync<NotSupportedException>(() => host.StartAsync(token));
        Assert.Contains("does not stream", startup.Message, StringComparison.Ordinal);

        var calling = Host.CreateApplicationBuilder();
        calling
            .Services.AddHostLoom()
            .UseTransport<RequestOnlyBroker>()
            .AddStreamClient<ListCatalog, string>();
        using var caller = calling.Build();
        await caller.StartAsync(token);
        var client = caller.Services.GetRequiredService<
            IStreamRequestClient<ListCatalog, string>
        >();

        var rejected = Assert.Throws<NotSupportedException>(() =>
            client.StreamAsync("catalog", new ListCatalog("eu"), cancellationToken: token)
        );
        Assert.Contains(nameof(RequestOnlyBroker), rejected.Message, StringComparison.Ordinal);

        Assert.Throws<ArgumentNullException>(() =>
            client.StreamAsync("catalog", null!, cancellationToken: token)
        );
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            client.StreamAsync("catalog", new ListCatalog("eu"), TimeSpan.Zero, token)
        );
    }

    [Fact]
    public void A_message_type_cannot_be_registered_as_a_request_and_a_stream()
    {
        var services = new ServiceCollection();

        var exception = Assert.Throws<InvalidOperationException>(() =>
            services
                .AddHostLoom()
                .UseInMemory()
                .AddHandler<DualCatalog, string, DualCatalogHandler>("catalog")
                .AddStreamHandler<DualCatalog, string, DualCatalogStreamHandler>("catalog")
        );

        Assert.Contains(nameof(DualCatalog), exception.Message, StringComparison.Ordinal);
    }

    private static async IAsyncEnumerable<object?> Hold(
        [EnumeratorCancellation] CancellationToken cancellationToken
    )
    {
        yield return "books";
        await Task.Delay(Timeout.Infinite, cancellationToken);
    }

    public sealed record ListCatalog(string Region) : IStreamRequest<string>;

    public sealed record OtherPages(string Region) : IStreamRequest<string>;

    public sealed record Ping(string Region) : IRequest<string>;

    public sealed record DualCatalog(string Region) : IRequest<string>, IStreamRequest<string>;

    public sealed class PingHandler : IRequestHandler<Ping, string>
    {
        public ValueTask<string> HandleAsync(Ping request, CancellationToken cancellationToken) =>
            ValueTask.FromResult(request.Region);
    }

    public sealed class ListingCatalogHandler : IStreamRequestHandler<ListCatalog, string>
    {
        public async IAsyncEnumerable<string> HandleAsync(
            ListCatalog request,
            [EnumeratorCancellation] CancellationToken cancellationToken
        )
        {
            Assert.Equal("eu", request.Region);
            yield return "books";
            yield return "music";
            await Task.CompletedTask;
        }
    }

    public sealed class ScopedCatalogHandler(CatalogScope scope)
        : IStreamRequestHandler<ListCatalog, string>
    {
        public async IAsyncEnumerable<string> HandleAsync(
            ListCatalog request,
            [EnumeratorCancellation] CancellationToken cancellationToken
        )
        {
            var id = scope.Id.ToString(System.Globalization.CultureInfo.InvariantCulture);
            yield return id;
            yield return id;
            await Task.CompletedTask;
        }
    }

    public sealed class CatalogScope : IDisposable
    {
        private static int _next;
        private readonly List<string> _log;

        public CatalogScope(List<string> log)
        {
            _log = log;
            Id = Interlocked.Increment(ref _next);
            log.Add($"created:{Id}");
        }

        public int Id { get; }

        public void Dispose() => _log.Add($"disposed:{Id}");
    }

    public sealed class FailingCatalogHandler : IStreamRequestHandler<ListCatalog, string>
    {
        public async IAsyncEnumerable<string> HandleAsync(
            ListCatalog request,
            [EnumeratorCancellation] CancellationToken cancellationToken
        )
        {
            yield return "books";
            throw new InvalidOperationException("catalog unavailable");
        }
    }

    public sealed class HoldingCatalogHandler(TaskCompletionSource cancelled)
        : IStreamRequestHandler<ListCatalog, string>
    {
        public async IAsyncEnumerable<string> HandleAsync(
            ListCatalog request,
            [EnumeratorCancellation] CancellationToken cancellationToken
        )
        {
            yield return "books";
            try
            {
                await Task.Delay(Timeout.Infinite, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                cancelled.TrySetResult();
                throw;
            }
        }
    }

    public sealed class DualCatalogHandler : IRequestHandler<DualCatalog, string>
    {
        public ValueTask<string> HandleAsync(
            DualCatalog request,
            CancellationToken cancellationToken
        ) => ValueTask.FromResult(request.Region);
    }

    public sealed class DualCatalogStreamHandler : IStreamRequestHandler<DualCatalog, string>
    {
        public async IAsyncEnumerable<string> HandleAsync(
            DualCatalog request,
            [EnumeratorCancellation] CancellationToken cancellationToken
        )
        {
            yield return request.Region;
            await Task.CompletedTask;
        }
    }

    /// <summary>A request transport with no stream capability, so startup and the client can reject it.</summary>
    public sealed class RequestOnlyBroker : IRequestBroker
    {
        public ValueTask<IAsyncDisposable> ListenAsync(
            RequestAddress address,
            RequestFrameHandler handler,
            CancellationToken cancellationToken
        ) => throw new InvalidOperationException("This broker has no request listener.");

        public ValueTask<ReadOnlyMemory<byte>> RequestAsync(
            RequestAddress address,
            ReadOnlyMemory<byte> request,
            Guid requestId,
            TimeSpan timeout,
            CancellationToken cancellationToken
        ) => throw new InvalidOperationException("This broker does not send requests.");

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
