using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using HostLoom.Transport.InMemory;
using NSubstitute;
using Xunit;

namespace HostLoom.Tests;

public sealed class RequestResponseTests
{
    [Fact]
    public async Task Request_runs_behaviors_and_returns_typed_response()
    {
        var calls = new List<string>();
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(calls);
        builder.Services
            .AddHostLoom()
            .UseInMemory()
            .AddHandler<Greet, Greeting, GreetHandler>("greeter")
            .AddBehavior<Greet, Greeting, RecordingBehavior>();

        using var host = builder.Build();
        await host.StartAsync(TestContext.Current.CancellationToken);

        var client = host.Services.GetRequiredService<IRequestClient<Greet, Greeting>>();
        var response = await client.GetResponseAsync(
            "greeter",
            new Greet("Ada"),
            cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal("Hello, Ada!", response.Text);
        Assert.Equal(["before", "handler", "after"], calls);
    }

    [Fact]
    public async Task Handler_exception_is_returned_as_remote_fault()
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services
            .AddHostLoom()
            .UseInMemory()
            .AddHandler<Fail, Never, FailingHandler>("failures");

        using var host = builder.Build();
        await host.StartAsync(TestContext.Current.CancellationToken);
        var client = host.Services.GetRequiredService<IRequestClient<Fail, Never>>();

        var exception = await Assert.ThrowsAsync<RemoteRequestException>(async () =>
            await client.GetResponseAsync(
                "failures",
                new Fail(),
                cancellationToken: TestContext.Current.CancellationToken));

        Assert.Equal(typeof(InvalidOperationException).FullName, exception.ErrorType);
        Assert.Contains("deliberate", exception.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("   at ", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Handler_is_not_invoked_through_an_endpoint_it_was_not_registered_on()
    {
        var greeter = Substitute.For<IRequestHandler<Greet, Greeting>>();
        greeter
            .HandleAsync(Arg.Any<Greet>(), Arg.Any<CancellationToken>())
            .Returns(ValueTask.FromResult(new Greeting("must not run")));

        var builder = Host.CreateApplicationBuilder();
        builder.Services
            .AddHostLoom()
            .UseInMemory()
            .AddHandler<Greet, Greeting, GreetHandler>("greeter")
            .AddHandler<Fail, Never, FailingHandler>("failures");
        builder.Services.AddScoped<IRequestHandler<Greet, Greeting>>(_ => greeter);

        using var host = builder.Build();
        await host.StartAsync(TestContext.Current.CancellationToken);
        var client = host.Services.GetRequiredService<IRequestClient<Greet, Greeting>>();

        // "failures" is a live endpoint, but Greet is registered against "greeter".
        var exception = await Assert.ThrowsAsync<RemoteRequestException>(async () =>
            await client.GetResponseAsync(
                "failures",
                new Greet("Ada"),
                cancellationToken: TestContext.Current.CancellationToken));

        Assert.Contains("failures", exception.Message, StringComparison.Ordinal);
        _ = greeter.DidNotReceive().HandleAsync(Arg.Any<Greet>(), Arg.Any<CancellationToken>());
    }

    public sealed record Greet(string Name) : IRequest<Greeting>;

    public sealed record Greeting(string Text);

    public sealed record Fail : IRequest<Never>;

    public sealed record Never;

    public sealed class GreetHandler(List<string> calls) : IRequestHandler<Greet, Greeting>
    {
        public ValueTask<Greeting> HandleAsync(Greet request, CancellationToken cancellationToken)
        {
            calls.Add("handler");
            return ValueTask.FromResult(new Greeting($"Hello, {request.Name}!"));
        }
    }

    public sealed class RecordingBehavior(List<string> calls) : IRequestBehavior<Greet, Greeting>
    {
        public async ValueTask<Greeting> HandleAsync(
            Greet request,
            RequestHandlerDelegate<Greeting> next,
            CancellationToken cancellationToken)
        {
            calls.Add("before");
            var response = await next(cancellationToken);
            calls.Add("after");
            return response;
        }
    }

    public sealed class FailingHandler : IRequestHandler<Fail, Never>
    {
        public ValueTask<Never> HandleAsync(Fail request, CancellationToken cancellationToken) =>
            throw new InvalidOperationException("deliberate failure");
    }
}
