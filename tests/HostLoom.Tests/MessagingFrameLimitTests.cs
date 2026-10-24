using System.Text.Json;
using HostLoom.Transport.InMemory;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Xunit;

namespace HostLoom.Tests;

public sealed class MessagingFrameLimitTests
{
    private const int Limit = 1024;

    [Theory]
    [InlineData("Request", 1023)]
    [InlineData("Request", 1024)]
    [InlineData("Request", 1025)]
    [InlineData("Event", 1023)]
    [InlineData("Event", 1024)]
    [InlineData("Event", 1025)]
    [InlineData("Response", 1023)]
    [InlineData("Response", 1024)]
    [InlineData("Response", 1025)]
    [InlineData("Fault", 1023)]
    [InlineData("Fault", 1024)]
    [InlineData("Fault", 1025)]
    public async Task Every_envelope_ingress_honors_the_inclusive_encoded_frame_limit(string kind, int bytes)
    {
        await using var frames = new WireEnvelopeValidationTests.FrameBroker();
        var ledger = new Ledger();
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton<IRequestBroker>(frames);
        builder.Services.AddSingleton(ledger);
        builder.Services.AddHostLoom(options => options.MaxFrameBytes = Limit)
            
            .AddHandler<Echo, string, EchoHandler>("catalog")
            .AddSubscriber<Placed, PlacedHandler>("orders", "audit");
        using var host = builder.Build();
        await host.StartAsync(TestContext.Current.CancellationToken);

        async Task ReceiveAsync()
        {
            switch (kind)
            {
                case "Request":
                    await frames.DeliverRequestAsync("catalog", Frame(kind, bytes));
                    break;
                case "Event":
                    await frames.DeliverEventAsync("orders", Frame(kind, bytes));
                    break;
                default:
                    frames.CannedReply = id => Frame(kind, bytes, id);
                    await host.Services.GetRequiredService<IRequestClient<Echo, string>>()
                        .GetResponseAsync("catalog", new Echo(), cancellationToken: TestContext.Current.CancellationToken);
                    break;
            }
        }

        if (bytes > Limit)
        {
            var error = await Assert.ThrowsAsync<MalformedEnvelopeException>(ReceiveAsync);
            Assert.Contains("MaxFrameBytes", error.Message, StringComparison.Ordinal);
            Assert.Null(error.InnerException);
            Assert.Equal(0, ledger.Handled);
        }
        else if (kind == "Fault")
            await Assert.ThrowsAsync<RemoteRequestException>(ReceiveAsync);
        else
            await ReceiveAsync();
        if (bytes <= Limit && kind is "Request" or "Event")
            Assert.Equal(1, ledger.Handled);
    }

    [Fact]
    public async Task An_oversized_invalid_frame_is_refused_before_JSON_decoding()
    {
        await using var frames = new WireEnvelopeValidationTests.FrameBroker();
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddHostLoom()
            .AddHandler<Echo, string, EchoHandler>("catalog");
        builder.Services.AddSingleton<IRequestBroker>(frames);
        builder.Services.AddSingleton<Ledger>();
        using var host = builder.Build();
        await host.StartAsync(TestContext.Current.CancellationToken);
        var error = await Assert.ThrowsAsync<MalformedEnvelopeException>(async () =>
            await frames.DeliverRequestAsync("catalog", new byte[HostLoomOptions.DefaultMaxFrameBytes + 1]));
        Assert.Contains("MaxFrameBytes", error.Message, StringComparison.Ordinal);
        Assert.Null(error.InnerException);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public async Task A_nonpositive_frame_limit_fails_host_startup(int limit)
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddHostLoom(options => options.MaxFrameBytes = limit).UseInMemory();
        using var host = builder.Build();
        var error = await Assert.ThrowsAsync<OptionsValidationException>(() => host.StartAsync(TestContext.Current.CancellationToken));
        Assert.Contains("MaxFrameBytes", error.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task In_memory_transport_rejects_oversized_frames_before_invoking_raw_handlers(bool eventFrame)
    {
        var services = new ServiceCollection();
        services.AddHostLoom(options => options.MaxFrameBytes = 32).UseInMemory();
        await using var provider = services.BuildServiceProvider();
        var broker = provider.GetRequiredService<IRequestBroker>();
        var events = Assert.IsType<InMemoryRequestBroker>(broker);
        var handled = 0;
        await using var listener = eventFrame
            ? await events.SubscribeAsync("orders", "audit", (_, _) => { handled++; return ValueTask.CompletedTask; }, TestContext.Current.CancellationToken)
            : await broker.ListenAsync("orders", (_, _) => { handled++; return ValueTask.FromResult(ReadOnlyMemory<byte>.Empty); }, TestContext.Current.CancellationToken);
        async Task SendAsync(int size)
        {
            if (eventFrame)
                await events.PublishAsync("orders", new byte[size], TestContext.Current.CancellationToken);
            else
                await broker.RequestAsync("orders", new byte[size], Guid.NewGuid(), TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        }
        await Assert.ThrowsAsync<MalformedEnvelopeException>(() => SendAsync(33));
        Assert.Equal(0, handled);
        await SendAsync(32);
        Assert.Equal(1, handled);
    }

    [Fact]
    public async Task In_memory_reply_frames_are_bounded_before_returning_to_a_raw_caller()
    {
        await using var broker = new InMemoryRequestBroker(messagingOptions: Options.Create(new HostLoomOptions { MaxFrameBytes = 32 }));
        await using var listener = await broker.ListenAsync("orders", (_, _) => ValueTask.FromResult<ReadOnlyMemory<byte>>(new byte[33]), TestContext.Current.CancellationToken);
        await Assert.ThrowsAsync<MalformedEnvelopeException>(async () =>
            await broker.RequestAsync("orders", new byte[1], Guid.NewGuid(), TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken));
    }

    private static byte[] Frame(string kind, int size, Guid? correlationId = null)
    {
        var type = kind switch { "Request" => typeof(Echo), "Event" => typeof(Placed), _ => typeof(string) };
        var raw = JsonSerializer.SerializeToUtf8Bytes(new
        {
            messageId = Guid.NewGuid(), correlationId, kind,
            messageType = $"{type.Assembly.GetName().Name}:{type.FullName}",
            responseType = $"{typeof(string).Assembly.GetName().Name}:{typeof(string).FullName}",
            sentAt = DateTimeOffset.UnixEpoch,
            body = kind == "Response" ? JsonSerializer.SerializeToUtf8Bytes("ok") : "{}"u8.ToArray(),
            fault = kind == "Fault" ? new { errorType = "HandlerFault", message = "failed" } : null,
        });
        var padded = new byte[size];
        padded.AsSpan().Fill((byte)' ');
        raw.CopyTo(padded, 0);
        return padded;
    }

    public sealed record Echo : IRequest<string>;
    public sealed record Placed : IEvent;
    public sealed class Ledger { public int Handled { get; set; } }
    public sealed class EchoHandler(Ledger ledger) : IRequestHandler<Echo, string>
    {
        public ValueTask<string> HandleAsync(Echo request, CancellationToken cancellationToken) { ledger.Handled++; return ValueTask.FromResult("ok"); }
    }
    public sealed class PlacedHandler(Ledger ledger) : IEventHandler<Placed>
    {
        public ValueTask HandleAsync(Placed @event, CancellationToken cancellationToken) { ledger.Handled++; return ValueTask.CompletedTask; }
    }
}
