using HostLoom.Transport.InMemory;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
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
        builder
            .Services.AddHostLoom()
            .UseInMemory()
            .AddHandler<Greet, Greeting, GreetHandler>("greeter")
            .AddBehavior<Greet, Greeting, RecordingBehavior>();

        using var host = builder.Build();
        await host.StartAsync(TestContext.Current.CancellationToken);

        var client = host.Services.GetRequiredService<IRequestClient<Greet, Greeting>>();
        var response = await client.GetResponseAsync(
            "greeter",
            new Greet("Ada"),
            cancellationToken: TestContext.Current.CancellationToken
        );

        Assert.Equal("Hello, Ada!", response.Text);
        Assert.Equal(["before", "handler", "after"], calls);
    }

    [Fact]
    public async Task Handler_exception_is_returned_as_remote_fault()
    {
        var builder = Host.CreateApplicationBuilder();
        builder
            .Services.AddHostLoom()
            .UseInMemory()
            .AddHandler<Fail, Never, FailingHandler>("failures");

        using var host = builder.Build();
        await host.StartAsync(TestContext.Current.CancellationToken);
        var client = host.Services.GetRequiredService<IRequestClient<Fail, Never>>();

        var exception = await Assert.ThrowsAsync<RemoteRequestException>(async () =>
            await client.GetResponseAsync(
                "failures",
                new Fail(),
                cancellationToken: TestContext.Current.CancellationToken
            )
        );

        // The handler's exception type and message stay on the handling side by default; the
        // caller learns only that the handler failed.
        Assert.Equal("HandlerFault", exception.ErrorType);
        Assert.Contains("The request handler failed.", exception.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("deliberate", exception.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(
            nameof(InvalidOperationException),
            exception.Message,
            StringComparison.Ordinal
        );
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
        builder
            .Services.AddHostLoom()
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
                cancellationToken: TestContext.Current.CancellationToken
            )
        );

        Assert.Contains("failures", exception.Message, StringComparison.Ordinal);
        _ = greeter.DidNotReceive().HandleAsync(Arg.Any<Greet>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Open_behavior_wraps_every_matching_request_type()
    {
        var calls = new List<string>();
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(calls);
        builder
            .Services.AddHostLoom()
            .UseInMemory()
            .AddHandler<Greet, Greeting, GreetHandler>("greeter")
            .AddHandler<LookupRate, RateQuote, LookupRateHandler>("rates")
            .AddOpenBehavior(typeof(RecordingOpenBehavior<,>));

        using var host = builder.Build();
        await host.StartAsync(TestContext.Current.CancellationToken);

        var greeter = host.Services.GetRequiredService<IRequestClient<Greet, Greeting>>();
        var greeting = await greeter.GetResponseAsync(
            "greeter",
            new Greet("Ada"),
            cancellationToken: TestContext.Current.CancellationToken
        );
        var rates = host.Services.GetRequiredService<IRequestClient<LookupRate, RateQuote>>();
        var quote = await rates.GetResponseAsync(
            "rates",
            new LookupRate("EUR/USD"),
            cancellationToken: TestContext.Current.CancellationToken
        );

        Assert.Equal("Hello, Ada!", greeting.Text);
        Assert.Equal(1.08m, quote.Value);
        Assert.Equal(
            [
                "open-before:Greet",
                "handler",
                "open-after:Greet",
                "open-before:LookupRate",
                "open-after:LookupRate",
            ],
            calls
        );
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Open_and_closed_behaviors_run_in_registration_order(bool openFirst)
    {
        var calls = new List<string>();
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(calls);
        var hostLoom = builder
            .Services.AddHostLoom()
            .UseInMemory()
            .AddHandler<Greet, Greeting, GreetHandler>("greeter");
        if (openFirst)
        {
            hostLoom
                .AddOpenBehavior(typeof(RecordingOpenBehavior<,>))
                .AddBehavior<Greet, Greeting, RecordingBehavior>();
        }
        else
        {
            hostLoom
                .AddBehavior<Greet, Greeting, RecordingBehavior>()
                .AddOpenBehavior(typeof(RecordingOpenBehavior<,>));
        }

        using var host = builder.Build();
        await host.StartAsync(TestContext.Current.CancellationToken);
        var client = host.Services.GetRequiredService<IRequestClient<Greet, Greeting>>();

        await client.GetResponseAsync(
            "greeter",
            new Greet("Ada"),
            cancellationToken: TestContext.Current.CancellationToken
        );

        Assert.Equal(
            openFirst
                ? ["open-before:Greet", "before", "handler", "after", "open-after:Greet"]
                : ["before", "open-before:Greet", "handler", "open-after:Greet", "after"],
            calls
        );
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task An_incompatible_open_behavior_fails_before_the_handler_runs(
        bool behaviorFirst
    )
    {
        var calls = new List<string>();
        var rates = Substitute.For<IRequestHandler<LookupRate, RateQuote>>();
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(calls);
        var hostLoom = builder.Services.AddHostLoom().UseInMemory();
        if (behaviorFirst)
        {
            hostLoom.AddOpenBehavior(typeof(ValidatedBehavior<,>));
        }

        hostLoom
            .AddHandler<Greet, Greeting, GreetHandler>("greeter")
            .AddHandler<LookupRate, RateQuote, LookupRateHandler>("rates");
        if (!behaviorFirst)
        {
            hostLoom.AddOpenBehavior(typeof(ValidatedBehavior<,>));
        }
        builder.Services.AddScoped<IRequestHandler<LookupRate, RateQuote>>(_ => rates);

        using var host = builder.Build();
        await host.StartAsync(TestContext.Current.CancellationToken);
        var greeter = host.Services.GetRequiredService<IRequestClient<Greet, Greeting>>();
        var greeting = await greeter.GetResponseAsync(
            "greeter",
            new Greet("Ada"),
            cancellationToken: TestContext.Current.CancellationToken
        );
        Assert.Equal("Hello, Ada!", greeting.Text);
        Assert.Equal(["validated", "handler"], calls);

        var client = host.Services.GetRequiredService<IRequestClient<LookupRate, RateQuote>>();
        var exception = await Assert.ThrowsAsync<RemoteRequestException>(async () =>
            await client.GetResponseAsync(
                "rates",
                new LookupRate("EUR/USD"),
                cancellationToken: TestContext.Current.CancellationToken
            )
        );
        Assert.Equal("HandlerFault", exception.ErrorType);
        _ = rates.DidNotReceive().HandleAsync(Arg.Any<LookupRate>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public void AddOpenBehavior_rejects_closed_or_unrelated_types()
    {
        var builder = Host.CreateApplicationBuilder();
        var hostLoom = builder.Services.AddHostLoom();

        Assert.Throws<ArgumentException>(() => hostLoom.AddOpenBehavior(typeof(RecordingBehavior)));
        Assert.Throws<ArgumentException>(() => hostLoom.AddOpenBehavior(typeof(string)));
        Assert.Throws<ArgumentException>(() =>
            hostLoom.AddOpenBehavior(typeof(AbstractBehavior<,>))
        );
        Assert.Throws<ArgumentException>(() =>
            hostLoom.AddOpenBehavior(typeof(SingleTypeBehavior<>))
        );
        Assert.Throws<ArgumentNullException>(() => hostLoom.AddOpenBehavior(null!));
    }

    public interface IValidatedRequest;

    public sealed record Greet(string Name) : IRequest<Greeting>, IValidatedRequest;

    public sealed record Greeting(string Text);

    public sealed record Fail : IRequest<Never>;

    public sealed record Never;

    public sealed record LookupRate(string Pair) : IRequest<RateQuote>;

    public sealed record RateQuote(decimal Value);

    public abstract class AbstractBehavior<TRequest, TResponse>
        : IRequestBehavior<TRequest, TResponse>
        where TRequest : IRequest<TResponse>
    {
        public abstract ValueTask<TResponse> HandleAsync(
            TRequest request,
            RequestHandlerDelegate<TResponse> next,
            CancellationToken cancellationToken
        );
    }

    public sealed class SingleTypeBehavior<TRequest> : IRequestBehavior<TRequest, Greeting>
        where TRequest : IRequest<Greeting>
    {
        public ValueTask<Greeting> HandleAsync(
            TRequest request,
            RequestHandlerDelegate<Greeting> next,
            CancellationToken cancellationToken
        ) => next(cancellationToken);
    }

    public sealed class ValidatedBehavior<TRequest, TResponse>(List<string> calls)
        : IRequestBehavior<TRequest, TResponse>
        where TRequest : class, IRequest<TResponse>, IValidatedRequest
    {
        public ValueTask<TResponse> HandleAsync(
            TRequest request,
            RequestHandlerDelegate<TResponse> next,
            CancellationToken cancellationToken
        )
        {
            calls.Add("validated");
            return next(cancellationToken);
        }
    }

    public sealed class LookupRateHandler : IRequestHandler<LookupRate, RateQuote>
    {
        public ValueTask<RateQuote> HandleAsync(
            LookupRate request,
            CancellationToken cancellationToken
        ) => ValueTask.FromResult(new RateQuote(1.08m));
    }

    public sealed class RecordingOpenBehavior<TRequest, TResponse>(List<string> calls)
        : IRequestBehavior<TRequest, TResponse>
        where TRequest : class, IRequest<TResponse>
    {
        public async ValueTask<TResponse> HandleAsync(
            TRequest request,
            RequestHandlerDelegate<TResponse> next,
            CancellationToken cancellationToken
        )
        {
            calls.Add($"open-before:{typeof(TRequest).Name}");
            var response = await next(cancellationToken);
            calls.Add($"open-after:{typeof(TRequest).Name}");
            return response;
        }
    }

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
            CancellationToken cancellationToken
        )
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
