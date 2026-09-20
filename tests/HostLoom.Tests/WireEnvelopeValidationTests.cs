using System.Text;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace HostLoom.Tests;

/// <summary>
/// The envelope identifiers a sender can leave out or zero. A missing <c>messageId</c> reads as
/// <see cref="Guid.Empty"/>, and every inbox key and correlation lookup is built on that id, so the
/// codec rejects it as malformed rather than letting unrelated deliveries collide.
/// </summary>
public sealed class WireEnvelopeValidationTests
{
    private static readonly string EchoType = TypeName<Echo>();
    private static readonly string EchoedType = TypeName<Echoed>();

    // An all-zero id passes deserialization and is caught by the codec's own check; a field that
    // is missing altogether fails deserialization, because the property is required. Both are
    // malformed, and neither reaches a handler.
    [Theory]
    [InlineData("\"messageId\":\"00000000-0000-0000-0000-000000000000\",", "message id")]
    [InlineData("", "message envelope")]
    public async Task A_request_without_a_message_id_is_malformed_and_never_handled(
        string idField,
        string reason
    )
    {
        var handled = 0;
        using var host = await StartAsync(() => handled++);
        var frame = Frame(
            $$"""
            {
              {{idField}}
              "kind": "Request",
              "messageType": "{{EchoType}}",
              "responseType": "{{EchoedType}}",
              "sentAt": "2026-09-20T10:00:00Z",
              "body": "{{Body(new Echo("a"))}}"
            }
            """
        );

        var exception = await Assert.ThrowsAsync<MalformedEnvelopeException>(async () =>
            await Broker(host).DeliverRequestAsync("echo", frame)
        );

        Assert.Contains(reason, exception.Message, StringComparison.Ordinal);
        Assert.Equal(0, handled);
    }

    [Fact]
    public async Task A_request_with_an_empty_correlation_id_is_malformed()
    {
        using var host = await StartAsync(() => { });
        var frame = Frame(
            $$"""
            {
              "messageId": "{{Guid.NewGuid()}}",
              "correlationId": "00000000-0000-0000-0000-000000000000",
              "kind": "Request",
              "messageType": "{{EchoType}}",
              "responseType": "{{EchoedType}}",
              "sentAt": "2026-09-20T10:00:00Z",
              "body": "{{Body(new Echo("a"))}}"
            }
            """
        );

        var exception = await Assert.ThrowsAsync<MalformedEnvelopeException>(async () =>
            await Broker(host).DeliverRequestAsync("echo", frame)
        );

        Assert.Contains("correlation id", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task An_event_without_a_message_id_is_malformed_and_never_handled()
    {
        var handled = 0;
        using var host = await StartAsync(() => handled++);
        var frame = Frame(
            $$"""
            {
              "kind": "Event",
              "messageType": "{{TypeName<Placed>()}}",
              "responseType": "",
              "sentAt": "2026-09-20T10:00:00Z",
              "body": "{{Body(new Placed("A-1"))}}"
            }
            """
        );

        await Assert.ThrowsAsync<MalformedEnvelopeException>(async () =>
            await Broker(host).DeliverEventAsync("orders", frame)
        );

        Assert.Equal(0, handled);
    }

    [Theory]
    [InlineData("", "correlation id of its request")]
    [InlineData(
        "\"correlationId\":\"00000000-0000-0000-0000-000000000000\",",
        "empty correlation id"
    )]
    public async Task A_response_without_a_usable_correlation_id_is_malformed(
        string correlationField,
        string reason
    )
    {
        using var host = await StartAsync(() => { });
        Broker(host).CannedResponse = Frame(
            $$"""
            {
              "messageId": "{{Guid.NewGuid()}}",
              {{correlationField}}
              "kind": "Response",
              "messageType": "{{EchoedType}}",
              "responseType": "{{EchoedType}}",
              "sentAt": "2026-09-20T10:00:00Z",
              "body": "{{Body(new Echoed("a"))}}"
            }
            """
        );
        var client = host.Services.GetRequiredService<IRequestClient<Echo, Echoed>>();

        var exception = await Assert.ThrowsAsync<MalformedEnvelopeException>(async () =>
            await client.GetResponseAsync(
                "echo",
                new Echo("a"),
                cancellationToken: TestContext.Current.CancellationToken
            )
        );

        Assert.Contains(reason, exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task A_well_formed_request_still_round_trips()
    {
        var handled = 0;
        using var host = await StartAsync(() => handled++);
        var client = host.Services.GetRequiredService<IRequestClient<Echo, Echoed>>();

        var echoed = await client.GetResponseAsync(
            "echo",
            new Echo("ok"),
            cancellationToken: TestContext.Current.CancellationToken
        );

        Assert.Equal("ok", echoed.Value);
        Assert.Equal(1, handled);
    }

    private static string TypeName<T>() =>
        $"{typeof(T).Assembly.GetName().Name}:{typeof(T).FullName}";

    private static string Body<T>(T value) =>
        Convert.ToBase64String(JsonSerializer.SerializeToUtf8Bytes(value));

    private static byte[] Frame(string json) => Encoding.UTF8.GetBytes(json);

    private static async Task<IHost> StartAsync(Action onHandled)
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(onHandled);
        builder.Services.AddSingleton<FrameBroker>();
        builder
            .Services.AddHostLoom()
            .UseTransport<FrameBroker>()
            .AddHandler<Echo, Echoed, EchoHandler>("echo")
            .AddSubscriber<Placed, PlacedHandler>("orders", "audit");
        var host = builder.Build();
        await host.StartAsync(TestContext.Current.CancellationToken);
        return host;
    }

    private static FrameBroker Broker(IHost host) =>
        (FrameBroker)host.Services.GetRequiredService<IRequestBroker>();

    public sealed record Echo(string Value) : IRequest<Echoed>;

    public sealed record Echoed(string Value);

    public sealed record Placed(string Reference) : IEvent;

    public sealed class EchoHandler(Action onHandled) : IRequestHandler<Echo, Echoed>
    {
        public ValueTask<Echoed> HandleAsync(Echo request, CancellationToken cancellationToken)
        {
            onHandled();
            return ValueTask.FromResult(new Echoed(request.Value));
        }
    }

    public sealed class PlacedHandler(Action onHandled) : IEventHandler<Placed>
    {
        public ValueTask HandleAsync(Placed @event, CancellationToken cancellationToken)
        {
            onHandled();
            return ValueTask.CompletedTask;
        }
    }

    /// <summary>
    /// A transport that hands raw frames to the registered handlers, and answers a request with
    /// a canned frame when one is set, so the codec's checks are exercised on both sides.
    /// </summary>
    public sealed class FrameBroker : IRequestBroker, IEventBroker
    {
        private readonly Dictionary<RequestAddress, RequestFrameHandler> _handlers = [];
        private readonly Dictionary<RequestAddress, List<EventFrameHandler>> _topics = [];

        public ReadOnlyMemory<byte>? CannedResponse { get; set; }

        public ValueTask<IAsyncDisposable> ListenAsync(
            RequestAddress address,
            RequestFrameHandler handler,
            CancellationToken cancellationToken
        )
        {
            _handlers[address] = handler;
            return ValueTask.FromResult<IAsyncDisposable>(Nothing.Instance);
        }

        public async ValueTask<ReadOnlyMemory<byte>> RequestAsync(
            RequestAddress address,
            ReadOnlyMemory<byte> request,
            Guid requestId,
            TimeSpan timeout,
            CancellationToken cancellationToken
        ) =>
            CannedResponse
            ?? await _handlers[address](request, cancellationToken).ConfigureAwait(false);

        public ValueTask<IAsyncDisposable> SubscribeAsync(
            RequestAddress topic,
            string subscription,
            EventFrameHandler handler,
            CancellationToken cancellationToken
        )
        {
            if (!_topics.TryGetValue(topic, out var handlers))
            {
                handlers = [];
                _topics[topic] = handlers;
            }

            handlers.Add(handler);
            return ValueTask.FromResult<IAsyncDisposable>(Nothing.Instance);
        }

        public ValueTask PublishAsync(
            RequestAddress topic,
            ReadOnlyMemory<byte> frame,
            CancellationToken cancellationToken
        ) => DeliverEventAsync(topic, frame);

        public ValueTask<ReadOnlyMemory<byte>> DeliverRequestAsync(
            RequestAddress address,
            ReadOnlyMemory<byte> frame
        ) => _handlers[address](frame, TestContext.Current.CancellationToken);

        public async ValueTask DeliverEventAsync(RequestAddress topic, ReadOnlyMemory<byte> frame)
        {
            foreach (var handler in _topics.GetValueOrDefault(topic) ?? [])
            {
                await handler(frame, TestContext.Current.CancellationToken).ConfigureAwait(false);
            }
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;

        private sealed class Nothing : IAsyncDisposable
        {
            public static readonly Nothing Instance = new();

            public ValueTask DisposeAsync() => ValueTask.CompletedTask;
        }
    }
}
