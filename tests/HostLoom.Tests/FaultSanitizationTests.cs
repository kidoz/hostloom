using System.Diagnostics.Metrics;
using HostLoom.Transport.InMemory;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace HostLoom.Tests;

/// <summary>
/// What a remote caller learns about a handler failure. By default nothing but "the handler
/// failed": internal exception types and messages stay on the handling side, in the log. A
/// <see cref="RemoteFaultException"/> is the deliberate exception, and
/// <see cref="HostLoomOptions.IncludeFaultDetails"/> the deployment-wide opt-in.
/// </summary>
public sealed class FaultSanitizationTests
{
    [Fact]
    public async Task An_ordinary_exception_reaches_the_caller_as_an_anonymous_handler_fault()
    {
        using var host = await StartAsync();

        var exception = await Assert.ThrowsAsync<RemoteRequestException>(async () =>
            await ClientOf<Fail, Never>(host)
                .GetResponseAsync(
                    "orders",
                    new Fail("connection string leaked"),
                    cancellationToken: TestContext.Current.CancellationToken
                )
        );

        Assert.Equal("HandlerFault", exception.ErrorType);
        Assert.Contains("The request handler failed.", exception.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("leaked", exception.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(
            nameof(InvalidOperationException),
            exception.Message,
            StringComparison.Ordinal
        );
    }

    [Fact]
    public async Task A_remote_fault_exception_carries_its_type_and_message_to_the_caller()
    {
        using var host = await StartAsync();

        var exception = await Assert.ThrowsAsync<RemoteRequestException>(async () =>
            await ClientOf<Decline, Never>(host)
                .GetResponseAsync(
                    "orders",
                    new Decline("order A-7 is already shipped"),
                    cancellationToken: TestContext.Current.CancellationToken
                )
        );

        Assert.Equal(typeof(OrderRejectedException).FullName, exception.ErrorType);
        Assert.Contains(
            "order A-7 is already shipped",
            exception.Message,
            StringComparison.Ordinal
        );
    }

    [Fact]
    public async Task Including_fault_details_forwards_every_exception_in_full()
    {
        using var host = await StartAsync(options => options.IncludeFaultDetails = true);

        var exception = await Assert.ThrowsAsync<RemoteRequestException>(async () =>
            await ClientOf<Fail, Never>(host)
                .GetResponseAsync(
                    "orders",
                    new Fail("trusted caller"),
                    cancellationToken: TestContext.Current.CancellationToken
                )
        );

        Assert.Equal(typeof(InvalidOperationException).FullName, exception.ErrorType);
        Assert.Contains("trusted caller", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task A_request_nobody_handles_is_a_stable_handler_not_found_fault_tagged_unknown()
    {
        using var recorder = new FaultTagRecorder();
        using var host = await StartAsync();

        // Greet is a live contract but is not registered on "orders", so the endpoint receives a
        // message type it does not know.
        var exception = await Assert.ThrowsAsync<RemoteRequestException>(async () =>
            await ClientOf<Greet, Greeting>(host)
                .GetResponseAsync(
                    "orders",
                    new Greet("Ada"),
                    cancellationToken: TestContext.Current.CancellationToken
                )
        );

        Assert.Equal("HandlerNotFound", exception.ErrorType);
        Assert.Contains("orders", exception.Message, StringComparison.Ordinal);
        // The wire message type is caller-controlled, so it must not become a metric tag value.
        var tag = Assert.Single(recorder.FaultMessageTypes);
        Assert.Equal("unknown", tag);
    }

    private static async Task<IHost> StartAsync(Action<HostLoomOptions>? configure = null)
    {
        var builder = Host.CreateApplicationBuilder();
        builder
            .Services.AddHostLoom(configure)
            .UseInMemory()
            .AddHandler<Fail, Never, FailingHandler>("orders")
            .AddHandler<Decline, Never, DecliningHandler>("orders")
            .AddHandler<Greet, Greeting, GreetHandler>("greetings");
        var host = builder.Build();
        await host.StartAsync(TestContext.Current.CancellationToken);
        return host;
    }

    private static IRequestClient<TRequest, TResponse> ClientOf<TRequest, TResponse>(IHost host)
        where TRequest : IRequest<TResponse> =>
        host.Services.GetRequiredService<IRequestClient<TRequest, TResponse>>();

    public sealed record Fail(string Secret) : IRequest<Never>;

    public sealed record Decline(string Reason) : IRequest<Never>;

    public sealed record Greet(string Name) : IRequest<Greeting>;

    public sealed record Greeting(string Text);

    public sealed record Never;

    public sealed class FailingHandler : IRequestHandler<Fail, Never>
    {
        public ValueTask<Never> HandleAsync(Fail request, CancellationToken cancellationToken) =>
            throw new InvalidOperationException(request.Secret);
    }

    public sealed class OrderRejectedException(string message) : RemoteFaultException(message);

    public sealed class DecliningHandler : IRequestHandler<Decline, Never>
    {
        public ValueTask<Never> HandleAsync(Decline request, CancellationToken cancellationToken) =>
            throw new OrderRejectedException(request.Reason);
    }

    public sealed class GreetHandler : IRequestHandler<Greet, Greeting>
    {
        public ValueTask<Greeting> HandleAsync(
            Greet request,
            CancellationToken cancellationToken
        ) => ValueTask.FromResult(new Greeting($"Hello, {request.Name}!"));
    }

    /// <summary>
    /// Collects the <c>messaging.message.type</c> tag of every request fault counted on "orders".
    /// Other test classes fail events on an "orders" topic in parallel, and the meter is
    /// process-wide, so the kind tag keeps their faults out.
    /// </summary>
    private sealed class FaultTagRecorder : IDisposable
    {
        private readonly MeterListener _listener = new();
        private readonly Lock _gate = new();
        private readonly List<string> _types = [];

        public FaultTagRecorder()
        {
            _listener.InstrumentPublished = (instrument, listener) =>
            {
                if (
                    instrument.Meter.Name == HostLoomDiagnostics.MeterName
                    && instrument.Name == "hostloom.request.faults"
                )
                {
                    listener.EnableMeasurementEvents(instrument);
                }
            };
            _listener.SetMeasurementEventCallback<long>(
                (_, _, tags, _) =>
                {
                    string? destination = null;
                    string? type = null;
                    string? kind = null;
                    foreach (var tag in tags)
                    {
                        switch (tag.Key)
                        {
                            case "messaging.destination.name":
                                destination = tag.Value as string;
                                break;
                            case "messaging.message.type":
                                type = tag.Value as string;
                                break;
                            case "hostloom.message.kind":
                                kind = tag.Value as string;
                                break;
                        }
                    }

                    if (destination == "orders" && kind == "request" && type is not null)
                    {
                        lock (_gate)
                        {
                            _types.Add(type);
                        }
                    }
                }
            );
            _listener.Start();
        }

        public IReadOnlyList<string> FaultMessageTypes
        {
            get
            {
                lock (_gate)
                {
                    return [.. _types];
                }
            }
        }

        public void Dispose() => _listener.Dispose();
    }
}
