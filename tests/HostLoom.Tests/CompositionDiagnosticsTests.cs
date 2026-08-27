using HostLoom.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace HostLoom.Tests;

public sealed class CompositionDiagnosticsTests
{
    [Fact]
    public void Ledger_is_shared_by_every_registration_and_resolves_from_the_container()
    {
        var services = new ServiceCollection();

        var first = services.CompositionLedger();
        var second = services.CompositionLedger();
        first.Record("Transport", "Kafka");

        using var provider = services.BuildServiceProvider();
        Assert.Same(first, second);
        Assert.Same(first, provider.GetRequiredService<CompositionLedger>());
        Assert.Single(services, descriptor => descriptor.ServiceType == typeof(CompositionLedger));
    }

    [Fact]
    public void Skipped_components_are_recorded_so_an_absence_is_visible()
    {
        var services = new ServiceCollection();
        services.RecordComposition("Transport", "Kafka", "Transport:Kafka:Enabled=true");
        services.RecordSkippedComposition("Outbox", "no Outbox section bound");

        var report = services.CompositionLedger().Snapshot();

        Assert.Equal("Transport=Kafka | Outbox=(skipped)", report.Describe());
        Assert.True(report.Decisions[1].IsSkipped);
        Assert.Empty(report.Conflicts);
    }

    [Fact]
    public void Origin_names_the_registration_method_that_recorded_the_decision()
    {
        var services = new ServiceCollection();

        AddTransport(services);

        var decision = Assert.Single(services.CompositionLedger().Snapshot().Decisions);
        // Captured at the caller of RecordComposition, not inside it: the useful answer to
        // "which branch activated" is the registration method, not the ledger's own plumbing.
        Assert.Equal(nameof(AddTransport), decision.Origin);
    }

    [Fact]
    public void Conflicting_choices_for_one_component_are_reported_without_naming_a_winner()
    {
        var ledger = new CompositionLedger();
        ledger.Record("Transport", "Kafka");
        ledger.Record("Transport", "InMemory");
        ledger.Record("Outbox", "Sql");
        ledger.Record("Outbox", "Sql");

        var report = ledger.Snapshot();

        var conflict = Assert.Single(report.Conflicts);
        Assert.Equal("Transport", conflict.Component);
        Assert.Equal(["Kafka", "InMemory"], conflict.Choices);
        // The same component recorded twice with the same choice is a duplicate call, not a
        // disagreement, so it must not raise a warning nobody can act on.
        Assert.Equal(
            "Transport=Kafka | Transport=InMemory | Outbox=Sql | Outbox=Sql",
            report.Describe()
        );
    }

    [Fact]
    public void Recording_a_decision_writes_nothing_until_the_application_opts_in()
    {
        var services = new ServiceCollection();

        services.RecordComposition("Transport", "Kafka");

        // Collection is unconditional; reporting is not. A library recording a decision must not
        // start a reporter in an application that never asked for one.
        Assert.DoesNotContain(
            services,
            descriptor => descriptor.ServiceType == typeof(IHostedService)
        );
    }

    [Fact]
    public void Adding_diagnostics_twice_registers_a_single_reporter()
    {
        var services = new ServiceCollection();

        services.AddCompositionDiagnostics();
        services.AddCompositionDiagnostics();

        Assert.Single(services, descriptor => descriptor.ServiceType == typeof(IHostedService));
    }

    [Fact]
    public async Task Host_start_reports_the_manifest_once_regardless_of_opt_in_order()
    {
        using var recorder = new RecordingLoggerProvider();
        var builder = Host.CreateApplicationBuilder();
        ConfigureLogging(builder, recorder);
        builder.Services.RecordComposition(
            "Transport",
            "InMemory",
            "Transport:Kafka:Enabled=false"
        );
        // Opting in after a decision was already recorded must not lose it: the report is taken at
        // startup, not at the moment diagnostics are switched on.
        builder.Services.AddCompositionDiagnostics();
        builder.Services.RecordSkippedComposition("Outbox", "no Outbox section bound");

        using var host = builder.Build();
        await host.StartAsync(TestContext.Current.CancellationToken);
        await host.StopAsync(TestContext.Current.CancellationToken);

        var manifest = Assert.Single(Composition(recorder, LogLevel.Information));
        Assert.Equal(
            "HostLoom composition: Transport=InMemory | Outbox=(skipped)",
            manifest.Message
        );
        Assert.Equal(2, Composition(recorder, LogLevel.Debug).Count);
        Assert.Contains(
            Composition(recorder, LogLevel.Debug),
            entry =>
                entry.Message
                == "HostLoom composition Outbox -> (skipped) recorded by "
                    + "Host_start_reports_the_manifest_once_regardless_of_opt_in_order: "
                    + "no Outbox section bound"
        );
        Assert.Empty(Composition(recorder, LogLevel.Warning));
    }

    [Fact]
    public async Task Host_start_warns_when_one_component_was_recorded_with_conflicting_choices()
    {
        using var recorder = new RecordingLoggerProvider();
        var builder = Host.CreateApplicationBuilder();
        ConfigureLogging(builder, recorder);
        builder.Services.AddCompositionDiagnostics();
        builder.Services.RecordComposition("Transport", "Kafka");
        builder.Services.RecordComposition("Transport", "InMemory");

        using var host = builder.Build();
        await host.StartAsync(TestContext.Current.CancellationToken);
        await host.StopAsync(TestContext.Current.CancellationToken);

        var warning = Assert.Single(Composition(recorder, LogLevel.Warning));
        Assert.Equal(
            "HostLoom composition component 'Transport' was recorded with conflicting choices: "
                + "Kafka, InMemory. Only one of them is in effect.",
            warning.Message
        );
    }

    [Fact]
    public void An_empty_ledger_reports_nothing()
    {
        using var recorder = new RecordingLoggerProvider();

        CompositionDiagnostics.Report(
            recorder.CreateLogger(CompositionDiagnostics.LogCategory),
            new CompositionLedger().Snapshot()
        );

        // An empty manifest line reads as "nothing was configured", which is a different claim.
        Assert.Empty(recorder.Entries);
    }

    [Fact]
    public void Recording_a_skip_through_Record_is_rejected()
    {
        var ledger = new CompositionLedger();

        // The public constant makes the skipped choice reachable from Record, where the reason is
        // optional — which would produce exactly the reasonless skip RecordSkipped forbids.
        var exception = Assert.Throws<ArgumentException>(() =>
            ledger.Record("Outbox", CompositionDecision.Skipped)
        );

        Assert.Equal("choice", exception.ParamName);
        Assert.Empty(ledger.Snapshot().Decisions);
    }

    [Fact]
    public void Two_reports_describing_the_same_composition_are_equal()
    {
        var first = new CompositionLedger();
        first.Record("Transport", "Kafka");
        first.Record("Transport", "InMemory");
        var second = new CompositionLedger();
        second.Record("Transport", "Kafka");
        second.Record("Transport", "InMemory");

        // The type is a record, so it advertises value equality; comparing the collections by
        // reference would silently break every Assert.Equal over a report.
        Assert.Equal(first.Snapshot(), second.Snapshot());
        Assert.Equal(first.Snapshot().GetHashCode(), second.Snapshot().GetHashCode());

        second.Record("Outbox", "Sql");
        Assert.NotEqual(first.Snapshot(), second.Snapshot());
    }

    [Fact]
    public void A_reported_snapshot_cannot_be_mutated_through_its_collections()
    {
        var ledger = new CompositionLedger();
        ledger.Record("Transport", "Kafka");
        ledger.Record("Transport", "InMemory");

        var report = ledger.Snapshot();

        // Handing out the backing array would let a consumer edit the decisions out from under the
        // conflicts computed from them, leaving a report that contradicts itself.
        Assert.Throws<NotSupportedException>(() =>
            ((IList<CompositionDecision>)report.Decisions).Clear()
        );
        Assert.Throws<NotSupportedException>(() =>
            ((IList<CompositionConflict>)report.Conflicts).Clear()
        );
        Assert.Throws<NotSupportedException>(() =>
            ((IList<string>)report.Conflicts[0].Choices).Clear()
        );
    }

    [Fact]
    public async Task A_logger_that_throws_does_not_take_the_host_down()
    {
        using var provider = new ThrowingLoggerProvider();
        var builder = Host.CreateApplicationBuilder();
        builder.Logging.ClearProviders();
        builder.Logging.AddProvider(provider);
        builder.Services.AddCompositionDiagnostics();
        builder.Services.RecordComposition("Transport", "Kafka");

        using var host = builder.Build();

        // Diagnostics are an aid. A broken logging provider must not be the reason a service
        // cannot start.
        await host.StartAsync(TestContext.Current.CancellationToken);
        await host.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public void Reporting_through_an_explicit_logger_stays_transparent()
    {
        var ledger = new CompositionLedger();
        ledger.Record("Transport", "Kafka");

        // The overload taking a logger is the caller's own call, so its failures are the caller's
        // to see; only the container-resolved path swallows.
        Assert.Throws<InvalidOperationException>(() =>
            CompositionDiagnostics.Report(new ThrowingLogger(), ledger.Snapshot())
        );
    }

    [Fact]
    public void Reporting_a_provider_without_a_ledger_or_logger_is_a_no_op()
    {
        var services = new ServiceCollection();
        services.RecordComposition("Transport", "Kafka");
        using var withoutLogging = services.BuildServiceProvider();
        using var withoutLedger = new ServiceCollection().BuildServiceProvider();

        CompositionDiagnostics.Report(withoutLogging);
        CompositionDiagnostics.Report(withoutLedger);
    }

    private static void AddTransport(IServiceCollection services) =>
        services.RecordComposition("Transport", "InMemory", "Transport:Kafka:Enabled=false");

    private static void ConfigureLogging(
        HostApplicationBuilder builder,
        RecordingLoggerProvider recorder
    )
    {
        builder.Logging.ClearProviders();
        builder.Logging.AddProvider(recorder);
        builder.Logging.SetMinimumLevel(LogLevel.Debug);
    }

    private static IReadOnlyList<RecordingLoggerProvider.Entry> Composition(
        RecordingLoggerProvider recorder,
        LogLevel level
    ) =>
        [
            .. recorder.Entries.Where(entry =>
                entry.Level == level && entry.Category == CompositionDiagnostics.LogCategory
            ),
        ];

    private sealed class ThrowingLoggerProvider : ILoggerProvider
    {
        // Only the composition category throws. A provider that fails on every category takes the
        // host down through its own "Application started" line, which would test nothing here.
        public ILogger CreateLogger(string categoryName) =>
            categoryName == CompositionDiagnostics.LogCategory
                ? new ThrowingLogger()
                : NullLogger.Instance;

        public void Dispose() { }
    }

    private sealed class ThrowingLogger : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter
        ) => throw new InvalidOperationException("The sink is unavailable.");
    }

    private sealed class RecordingLoggerProvider : ILoggerProvider
    {
        private readonly List<Entry> _entries = [];
        private readonly Lock _gate = new();

        // Host lifetime callbacks log from thread-pool threads, so an unguarded list can lose an
        // entry or throw out of the assertions enumerating it.
        public IReadOnlyList<Entry> Entries
        {
            get
            {
                lock (_gate)
                {
                    return [.. _entries];
                }
            }
        }

        public ILogger CreateLogger(string categoryName) => new RecordingLogger(this, categoryName);

        public void Dispose() { }

        private void Add(Entry entry)
        {
            lock (_gate)
            {
                _entries.Add(entry);
            }
        }

        public sealed record Entry(string Category, LogLevel Level, string Message);

        private sealed class RecordingLogger(RecordingLoggerProvider provider, string category)
            : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state)
                where TState : notnull => null;

            public bool IsEnabled(LogLevel logLevel) => logLevel != LogLevel.None;

            public void Log<TState>(
                LogLevel logLevel,
                EventId eventId,
                TState state,
                Exception? exception,
                Func<TState, Exception?, string> formatter
            ) => provider.Add(new Entry(category, logLevel, formatter(state, exception)));
        }
    }
}
