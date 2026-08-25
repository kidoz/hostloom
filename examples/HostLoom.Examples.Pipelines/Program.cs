using HostLoom.Pipelines;
using HostLoom.Pipelines.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace HostLoom.Examples.Pipelines;

internal static class Program
{
    public static async Task Main()
    {
        var builder = Host.CreateApplicationBuilder();

        // Filter dependencies register like any other service. AddPipeline owns private transient
        // registrations for its filters and resolves them per run through constructor injection.
        builder.Services.AddSingleton<IDocumentStore, LoggingDocumentStore>();
        builder.Services.AddSingleton<FeatureFlags>();
        builder.Services.AddTransient<WordCountFilter>();
        builder.Services.AddTransient<ReadingTimeFilter>();
        builder.Services.AddTransient<StoreDocumentFilter>();

        // One pipeline from several filters: stages execute in declared order, filters inside a
        // stage in registration order. WithTimeout/WithRetry wrap the whole run, first outermost,
        // so the timeout here is a budget across all retry attempts.
        builder.Services.AddPipeline<IndexingContext>(
            "document-indexing",
            pipeline =>
                pipeline
                    .WithTimeout(TimeSpan.FromMinutes(5))
                    .WithRetry(
                        RetryPolicy
                            .Exponential(2, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(10))
                            .WithJitter(0.2)
                    )
                    .Stage(
                        "analyze",
                        stage =>
                            stage
                                .AddFilter<WordCountFilter>(filter => filter.WithName("word_count"))
                                .AddFilter<SentenceCountFilter>(filter =>
                                    filter
                                        .WithName("sentence_count")
                                        .EnabledWhen(sp =>
                                            sp.GetRequiredService<FeatureFlags>().SentenceCountEnabled
                                        )
                                )
                    )
                    .Stage(
                        "summarize",
                        stage =>
                            stage.AddFilter<ReadingTimeFilter>(filter =>
                                filter.WithName("reading_minutes")
                            )
                    )
                    .Stage("store", stage => stage.AddFilter<StoreDocumentFilter>())
        );

        using var host = builder.Build();

        // Startup validates every registered pipeline (duplicate names, missing filter
        // dependencies) and logs each resolved topology before any run happens.
        await host.StartAsync().ConfigureAwait(false);

        var logger = host.Services.GetRequiredService<ILoggerFactory>().CreateLogger("Examples");
        await RunRegisteredPipelineAsync(host.Services, logger).ConfigureAwait(false);
        await RunManuallyComposedPipeAsync(host.Services, logger).ConfigureAwait(false);
        await RunStandalonePipeAsync(logger).ConfigureAwait(false);

        await host.StopAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// The registered pipeline: resolve the runner by pipeline name and send one context per
    /// batch. Each run gets its own dependency-injection scope and freshly resolved filters.
    /// </summary>
    private static async Task RunRegisteredPipelineAsync(IServiceProvider services, ILogger logger)
    {
        var runner = services.GetRequiredKeyedService<IPipelineRunner<IndexingContext>>(
            "document-indexing"
        );
        var topology = runner.Topology.Describe();
        logger.LogInformation("Registered pipeline: {Topology}", topology);

        await runner.RunAsync(new IndexingContext(Batch("intro"))).ConfigureAwait(false);

        // Toggles are evaluated per run: the next run composes the sentence_count filter in.
        services.GetRequiredService<FeatureFlags>().SentenceCountEnabled = true;
        logger.LogInformation("sentence_count enabled; indexing the next batch");
        await runner.RunAsync(new IndexingContext(Batch("guide"))).ConfigureAwait(false);
    }

    /// <summary>
    /// Composing a pipe yourself from container-resolved filters: useful when a host wants full
    /// control of composition but still wants constructor-injected filters. These filter types
    /// were registered explicitly for this manual path, so a scope hands out fresh instances.
    /// </summary>
    private static async Task RunManuallyComposedPipeAsync(
        IServiceProvider services,
        ILogger logger
    )
    {
        using var scope = services.CreateScope();
        var provider = scope.ServiceProvider;
        var pipe = Pipe.Create<IndexingContext>(builder =>
        {
            builder.UseTimeout(TimeSpan.FromSeconds(30));
            builder.Use(provider.GetRequiredService<WordCountFilter>());
            builder.Use(provider.GetRequiredService<ReadingTimeFilter>());
            builder.Use(provider.GetRequiredService<StoreDocumentFilter>());
        });

        var shape = string.Join(
            " -> ",
            PipelineProbe.Inspect(pipe).Children.Select(child => child.Name)
        );
        logger.LogInformation("Manually composed pipe: {Shape}", shape);
        await pipe.SendAsync(new IndexingContext(Batch("manual"))).ConfigureAwait(false);
    }

    /// <summary>
    /// No container at all: delegate filters and directly constructed filter instances compose
    /// the same way, which is also how filters are unit-tested.
    /// </summary>
    private static async Task RunStandalonePipeAsync(ILogger logger)
    {
        var pipe = Pipe.Create<IndexingContext>(builder =>
        {
            builder.UseRetry(RetryPolicy.Immediate(1));
            builder.Use(
                async (context, next) =>
                {
                    foreach (var document in context.Documents)
                    {
                        document.Metrics["word_count"] = document
                            .Content.Split(' ', StringSplitOptions.RemoveEmptyEntries)
                            .Length;
                    }

                    await next.SendAsync(context).ConfigureAwait(false);
                },
                "analyze"
            );
            builder.Use(new ReadingTimeFilter());
        });

        var shape = string.Join(
            " -> ",
            PipelineProbe.Inspect(pipe).Children.Select(child => child.Name)
        );
        logger.LogInformation("Standalone pipe: {Shape}", shape);

        var context = new IndexingContext(Batch("solo"));
        await pipe.SendAsync(context).ConfigureAwait(false);
        logger.LogInformation(
            "Standalone run summarized {Count} documents",
            context.Documents.Count(document => document.Insights.ContainsKey("reading_minutes"))
        );
    }

    private static IReadOnlyList<Document> Batch(string prefix) =>
        [
            new($"{prefix}-quickstart", "Getting started is easy. Install the tool. Run it once."),
            new($"{prefix}-note", "Short note."),
            new(
                $"{prefix}-manual",
                "The manual covers setup, configuration, and troubleshooting in depth. Read it fully before deploying. Then practice on a scratch environment."
            ),
        ];
}
