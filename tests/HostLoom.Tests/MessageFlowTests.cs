using System.Text.Json;
using HostLoom.Transport.InMemory;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

using HostLoom;

namespace HostLoom.Tests;

public sealed class MessageFlowTests
{
    [Fact]
    public async Task A_frame_without_flow_fields_still_runs_and_the_reply_joins_its_message_id()
    {
        using var host = await StartAsync();
        var broker = Broker(host);
        var messageId = Guid.NewGuid();
        var frame = Frame(
            $$"""
            {
              "messageId": "{{messageId}}",
              "kind": "Request",
              "messageType": "{{TypeName<Echo>()}}",
              "responseType": "{{TypeName<Echoed>()}}",
              "sentAt": "2026-09-20T10:00:00Z",
              "body": "{{Body(new Echo("a"))}}"
            }
            """
        );

        var response = JsonDocument.Parse(await broker.DeliverRequestAsync("echo", frame));

        Assert.Equal(1, Handled(host));
        Assert.Equal(messageId, response.RootElement.GetProperty("correlationId").GetGuid());
        Assert.Equal(messageId, response.RootElement.GetProperty("conversationId").GetGuid());
        Assert.Equal(messageId, response.RootElement.GetProperty("initiatorId").GetGuid());
        Assert.False(response.RootElement.TryGetProperty("expiresAt", out _));
    }

    [Theory]
    [InlineData("conversationId")]
    [InlineData("initiatorId")]
    public async Task An_empty_flow_id_is_malformed(string field)
    {
        using var host = await StartAsync();
        var frame = Frame(
            $$"""
            {
              "messageId": "{{Guid.NewGuid()}}",
              "{{field}}": "00000000-0000-0000-0000-000000000000",
              "kind": "Request",
              "messageType": "{{TypeName<Echo>()}}",
              "responseType": "{{TypeName<Echoed>()}}",
              "sentAt": "2026-09-20T10:00:00Z",
              "body": "{{Body(new Echo("a"))}}"
            }
            """
        );

        var exception = await Assert.ThrowsAsync<MalformedEnvelopeException>(async () =>
            await Broker(host).DeliverRequestAsync("echo", frame)
        );

        Assert.Contains(field == "conversationId" ? "conversation id" : "initiator id", exception.Message, StringComparison.Ordinal);
        Assert.Equal(0, Handled(host));
    }

    [Fact]
    public async Task An_expired_request_does_not_run_its_handler()
    {
        using var host = await StartAsync();
        var frame = Frame(
            $$"""
            {
              "messageId": "{{Guid.NewGuid()}}",
              "kind": "Request",
              "messageType": "{{TypeName<Echo>()}}",
              "responseType": "{{TypeName<Echoed>()}}",
              "sentAt": "2026-09-20T10:00:00Z",
              "expiresAt": "2000-01-01T00:00:00Z",
              "body": "{{Body(new Echo("a"))}}"
            }
            """
        );

        await Assert.ThrowsAsync<RequestExpiredException>(async () =>
            await Broker(host).DeliverRequestAsync("echo", frame)
        );
        Assert.Equal(0, Handled(host));
    }

    [Fact]
    public async Task An_in_memory_expired_request_times_out_without_a_handler()
    {
        var clock = new HostLoom.Conformance.ManualTimeProvider();
        await using var broker = new InMemoryRequestBroker(logger: null, timeProvider: clock);
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await broker.ListenAsync(
            "echo",
            (RequestFrameHandler)(
                (_, _) =>
                {
                    started.TrySetResult();
                    throw new RequestExpiredException("echo", clock.GetUtcNow());
                }
            ),
            TestContext.Current.CancellationToken
        );

        var waiting = broker.RequestAsync(
            "echo",
            "{}"u8.ToArray(),
            Guid.NewGuid(),
            TimeSpan.FromSeconds(5),
            TestContext.Current.CancellationToken
        );
        await started.Task.WaitAsync(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);
        clock.Advance(TimeSpan.FromSeconds(5));
        await Assert.ThrowsAsync<RequestTimeoutException>(async () => await waiting);
    }

    [Fact]
    public async Task A_publish_and_nested_request_continue_the_inbound_conversation()
    {
        using var host = await StartAsync();
        var broker = Broker(host);
        var messageId = Guid.NewGuid();
        var conversationId = Guid.NewGuid();
        var frame = Frame(
            $$"""
            {
              "messageId": "{{messageId}}",
              "conversationId": "{{conversationId}}",
              "kind": "Request",
              "messageType": "{{TypeName<Echo>()}}",
              "responseType": "{{TypeName<Echoed>()}}",
              "sentAt": "2026-09-20T10:00:00Z",
              "body": "{{Body(new Echo("continue"))}}"
            }
            """
        );

        await broker.DeliverRequestAsync("echo", frame);

        var nested = JsonDocument.Parse(Assert.Single(broker.Requests));
        var published = JsonDocument.Parse(Assert.Single(broker.Events));
        Assert.Equal(conversationId, nested.RootElement.GetProperty("conversationId").GetGuid());
        Assert.Equal(messageId, nested.RootElement.GetProperty("initiatorId").GetGuid());
        Assert.True(nested.RootElement.TryGetProperty("expiresAt", out _));
        Assert.Equal(conversationId, published.RootElement.GetProperty("conversationId").GetGuid());
        Assert.Equal(messageId, published.RootElement.GetProperty("initiatorId").GetGuid());
        Assert.False(published.RootElement.TryGetProperty("expiresAt", out _));
    }

    [Fact]
    public async Task A_publish_outside_a_delivery_starts_a_conversation()
    {
        using var host = await StartAsync();
        var endpoint = host.Services.GetRequiredService<IPublishEndpoint>();
        await endpoint.PublishAsync("orders", new Placed("n1"), TestContext.Current.CancellationToken);

        var published = JsonDocument.Parse(Assert.Single(Broker(host).Events));
        Assert.NotEqual(Guid.Empty, published.RootElement.GetProperty("conversationId").GetGuid());
        Assert.False(published.RootElement.TryGetProperty("initiatorId", out _));
    }

    [Fact]
    public async Task An_outbox_publish_keeps_the_inbound_conversation()
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(new Count());
        builder.Services.AddSingleton<RecordingBroker>();
        builder
            .Services.AddHostLoom()
            .UseTransport<RecordingBroker>()
            .UseInMemoryOutbox()
            .AddHandler<Echo, Echoed, ContinuingHandler>("echo")
            .AddHandler<Nested, NestedResult, NestedHandler>("nested");
        using var host = builder.Build();
        await host.StartAsync(TestContext.Current.CancellationToken);
        var broker = (RecordingBroker)host.Services.GetRequiredService<IRequestBroker>();
        var conversationId = Guid.NewGuid();
        var messageId = Guid.NewGuid();
        await broker.DeliverRequestAsync(
            "echo",
            Frame(
                $$"""
                {
                  "messageId": "{{messageId}}",
                  "conversationId": "{{conversationId}}",
                  "kind": "Request",
                  "messageType": "{{TypeName<Echo>()}}",
                  "responseType": "{{TypeName<Echoed>()}}",
                  "sentAt": "2026-09-20T10:00:00Z",
                  "body": "{{Body(new Echo("continue"))}}"
                }
                """
            )
        );

        var published = await broker.EventPublished.Task.WaitAsync(
            TimeSpan.FromSeconds(5),
            TestContext.Current.CancellationToken
        );
        var document = JsonDocument.Parse(published);
        Assert.Equal(conversationId, document.RootElement.GetProperty("conversationId").GetGuid());
        Assert.Equal(messageId, document.RootElement.GetProperty("initiatorId").GetGuid());
    }

    [Fact]
    public async Task A_handler_can_read_the_flow()
    {
        using var host = await StartAsync();
        var conversationId = Guid.NewGuid();
        await Broker(host).DeliverRequestAsync(
            "echo",
            Frame(
                $$"""
                {
                  "messageId": "{{Guid.NewGuid()}}",
                  "conversationId": "{{conversationId}}",
                  "kind": "Request",
                  "messageType": "{{TypeName<Echo>()}}",
                  "responseType": "{{TypeName<Echoed>()}}",
                  "sentAt": "2026-09-20T10:00:00Z",
                  "body": "{{Body(new Echo("read"))}}"
                }
                """
            )
        );

        Assert.Equal(conversationId, host.Services.GetRequiredService<Count>().Seen);
    }

    private static async Task<IHost> StartAsync()
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(new Count());
        builder.Services.AddSingleton<RecordingBroker>();
        builder
            .Services.AddHostLoom()
            .UseTransport<RecordingBroker>()
            .AddHandler<Echo, Echoed, ContinuingHandler>("echo")
            .AddHandler<Nested, NestedResult, NestedHandler>("nested")
            .AddSubscriber<Placed, PlacedHandler>("orders");
        var host = builder.Build();
        await host.StartAsync(TestContext.Current.CancellationToken);
        return host;
    }

    private static RecordingBroker Broker(IHost host) =>
        (RecordingBroker)host.Services.GetRequiredService<IRequestBroker>();

    private static int Handled(IHost host) => host.Services.GetRequiredService<Count>().Handled;

    private static string TypeName<T>() =>
        $"{typeof(T).Assembly.GetName().Name}:{typeof(T).FullName}";

    private static string Body<T>(T value) =>
        Convert.ToBase64String(JsonSerializer.SerializeToUtf8Bytes(value));

    private static byte[] Frame(string json) => System.Text.Encoding.UTF8.GetBytes(json);

    public sealed record Echo(string Value) : IRequest<Echoed>;

    public sealed record Echoed(string Value);

    public sealed record Nested(string Value) : IRequest<NestedResult>;

    public sealed record NestedResult(string Value);

    public sealed record Placed(string Reference) : IEvent;

    public sealed class Count
    {
        public int Handled { get; set; }

        public Guid Seen { get; set; }
    }

    public sealed class ContinuingHandler(
        Count count,
        IMessageFlow flow,
        IRequestClient<Nested, NestedResult> client,
        IPublishEndpoint publish
    ) : IRequestHandler<Echo, Echoed>
    {
        public async ValueTask<Echoed> HandleAsync(Echo request, CancellationToken cancellationToken)
        {
            count.Handled++;
            count.Seen = flow.ConversationId;
            if (request.Value == "continue")
            {
                await client.GetResponseAsync("nested", new Nested(request.Value), cancellationToken: cancellationToken);
                await publish.PublishAsync("orders", new Placed(request.Value), cancellationToken);
            }

            return new Echoed(request.Value);
        }
    }

    public sealed class NestedHandler : IRequestHandler<Nested, NestedResult>
    {
        public ValueTask<NestedResult> HandleAsync(Nested request, CancellationToken cancellationToken) =>
            ValueTask.FromResult(new NestedResult(request.Value));
    }

    public sealed class PlacedHandler : IEventHandler<Placed>
    {
        public ValueTask HandleAsync(Placed @event, CancellationToken cancellationToken) =>
            ValueTask.CompletedTask;
    }

    public sealed class RecordingBroker : IRequestBroker, IEventBroker
    {
        private readonly Dictionary<RequestAddress, RequestFrameHandler> _handlers = [];

        public List<byte[]> Requests { get; } = [];

        public List<byte[]> Events { get; } = [];

        public TaskCompletionSource<byte[]> EventPublished { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

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
        )
        {
            var copy = request.ToArray();
            Requests.Add(copy);
            return await _handlers[address](copy, cancellationToken).ConfigureAwait(false);
        }

        public ValueTask<IAsyncDisposable> SubscribeAsync(
            RequestAddress topic,
            string subscription,
            EventFrameHandler handler,
            CancellationToken cancellationToken
        ) => ValueTask.FromResult<IAsyncDisposable>(Nothing.Instance);

        public ValueTask PublishAsync(
            RequestAddress topic,
            ReadOnlyMemory<byte> frame,
            CancellationToken cancellationToken
        )
        {
            var copy = frame.ToArray();
            Events.Add(copy);
            EventPublished.TrySetResult(copy);
            return ValueTask.CompletedTask;
        }

        public ValueTask<ReadOnlyMemory<byte>> DeliverRequestAsync(
            RequestAddress address,
            ReadOnlyMemory<byte> frame
        ) => _handlers[address](frame, TestContext.Current.CancellationToken);

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;

        private sealed class Nothing : IAsyncDisposable
        {
            public static readonly Nothing Instance = new();

            public ValueTask DisposeAsync() => ValueTask.CompletedTask;
        }
    }
}
