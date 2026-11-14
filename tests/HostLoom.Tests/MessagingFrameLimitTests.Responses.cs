using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace HostLoom.Tests;

public sealed partial class MessagingFrameLimitTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Dispatcher_generated_responses_and_faults_obey_the_frame_limit(bool fault)
    {
        var token = TestContext.Current.CancellationToken;
        await using var broker = new WireEnvelopeValidationTests.FrameBroker();
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton<IRequestBroker>(broker);
        builder
            .Services.AddHostLoom(options => options.MaxFrameBytes = Limit)
            .AddHandler<VerboseRequest, string, VerboseHandler>("catalog");
        using var host = builder.Build();
        await host.StartAsync(token);
        var request = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(
            new
            {
                messageId = Guid.NewGuid(),
                kind = "Request",
                messageType = $"{typeof(VerboseRequest).Assembly.GetName().Name}:{typeof(VerboseRequest).FullName}",
                responseType = "System.Private.CoreLib:System.String",
                sentAt = DateTimeOffset.UtcNow,
                body = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(
                    new VerboseRequest(fault)
                ),
            }
        );
        var response = await broker.DeliverRequestAsync("catalog", request);
        Assert.InRange(response.Length, 1, Limit);
        using var decoded = System.Text.Json.JsonDocument.Parse(response);
        Assert.Equal("Fault", decoded.RootElement.GetProperty("kind").GetString());
        var message = decoded.RootElement.GetProperty("fault").GetProperty("message").GetString();
        Assert.NotNull(message);
        Assert.DoesNotContain(new string('x', 1024), message, StringComparison.Ordinal);
    }

    public sealed record VerboseRequest(bool Fault) : IRequest<string>;

    public sealed class VerboseHandler : IRequestHandler<VerboseRequest, string>
    {
        public ValueTask<string> HandleAsync(
            VerboseRequest request,
            CancellationToken cancellationToken
        ) =>
            request.Fault
                ? throw new RemoteFaultException(new string('x', 8192))
                : ValueTask.FromResult(new string('x', 8192));
    }
}
