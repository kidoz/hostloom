using System.Diagnostics;
using System.Globalization;
using System.Runtime.InteropServices;
using System.Text.Json;
using HostLoom.Composition.Testing;
using HostLoom.Diagnostics;
using HostLoom.Examples.CompositionDiagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace HostLoom.Composition.Benchmarks;

internal static class Program
{
    private static object? _sink;

    public static void Main(string[] args)
    {
        if (args[0] == "export")
        {
            GeneratorMeasurements.Run(int.Parse(args[1], CultureInfo.InvariantCulture), args[2]);
            return;
        }
        if (args[0] == "generator")
        {
            GeneratorMeasurements.Run(int.Parse(args[1], CultureInfo.InvariantCulture));
            return;
        }
        if (args[0] == "verify")
        {
            var plan = RuntimeFixture.CreatePlan();
            var expected = CompositionRegistrationShape.Project(plan.Probe());
            var handwritten = RuntimeFixture
                .Handwritten()
                .Select(static descriptor =>
                    CompositionRegistrationShape.FromDescriptor(descriptor)
                )
                .ToArray();
            var scan = RuntimeFixture
                .Scan()
                .Select(static descriptor =>
                    CompositionRegistrationShape.FromDescriptor(descriptor)
                )
                .ToArray();
            CompositionAssert.EquivalentRegistrations(expected, handwritten);
            CompositionAssert.EquivalentRegistrations(expected, scan);
            CompositionAssert.RegistrationSequence(expected, handwritten);
            if (expected.Count != 100)
                throw new InvalidOperationException("Fixture must contain 100 registrations.");
            Write(new { verified = true, count = expected.Count });
            return;
        }
        CompositionPlan? planForPhase = args[0]
            is "apply"
                or "probe"
                or "ledger-record"
                or "ledger-report"
            ? RuntimeFixture.CreatePlan()
            : null;
        CompositionApplicationReport? report = args[0] is "ledger-record" or "ledger-report"
            ? planForPhase!.ApplyTo(new ServiceCollection())
            : null;
        CompositionLedger? ledgerForReport = null;
        if (args[0] == "ledger-report")
        {
            ledgerForReport = new CompositionLedger();
            ApplicationCompositionLedger.Record(ledgerForReport, planForPhase!, report!);
        }
        var logger = new FormattingLogger();
        Func<object> action = args[0] switch
        {
            "plan" => RuntimeFixture.CreatePlan,
            "apply" => () => planForPhase!.ApplyTo(new ServiceCollection()),
            "probe" => () => planForPhase!.Probe(),
            "total" => () => RuntimeFixture.CreatePlan().ApplyTo(new ServiceCollection()),
            "handwritten" => RuntimeFixture.Handwritten,
            "scrutor" => RuntimeFixture.Scan,
            "ledger-record" => () =>
            {
                var ledger = new CompositionLedger();
                ApplicationCompositionLedger.Record(ledger, planForPhase!, report!);
                return ledger;
            },
            "ledger-report" => () =>
            {
                var snapshot = ledgerForReport!.Snapshot();
                CompositionDiagnostics.Report(logger, snapshot);
                return snapshot;
            },
            "total-ledger" => () =>
            {
                var plan = RuntimeFixture.CreatePlan();
                var applied = plan.ApplyTo(new ServiceCollection());
                var ledger = new CompositionLedger();
                ApplicationCompositionLedger.Record(ledger, plan, applied);
                var snapshot = ledger.Snapshot();
                CompositionDiagnostics.Report(logger, snapshot);
                return snapshot;
            },
            _ => throw new ArgumentException("Unknown runtime case."),
        };
        // Timer/managed-allocation instrumentation is initialized before the first observed call.
        _ = Stopwatch.GetTimestamp();
        _ = GC.GetAllocatedBytesForCurrentThread();
        Measurement cold = Measure(action, 1);
        for (var i = 0; i < 64; i++)
            _sink = action();
        var samples = new List<Measurement>();
        int iterations = args[0] == "probe" ? 100_000 : 32;
        for (var i = 0; i < 15; i++)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            samples.Add(Measure(action, iterations));
        }
        Write(
            new
            {
                name = args[0],
                environment = EnvironmentData(),
                cold,
                samples,
                iterations,
            }
        );
        GC.KeepAlive(_sink);
    }

    internal static Measurement Measure(Func<object> action, int iterations)
    {
        long allocated = GC.GetAllocatedBytesForCurrentThread();
        long started = Stopwatch.GetTimestamp();
        for (var i = 0; i < iterations; i++)
            _sink = action();
        long elapsed = Stopwatch.GetTimestamp() - started;
        long bytes = GC.GetAllocatedBytesForCurrentThread() - allocated;
        return new Measurement(
            elapsed * 1_000_000_000.0 / Stopwatch.Frequency / iterations,
            (double)bytes / iterations
        );
    }

    internal static object EnvironmentData() =>
        new
        {
            runtime = RuntimeInformation.FrameworkDescription,
            architecture = RuntimeInformation.ProcessArchitecture.ToString(),
            os = RuntimeInformation.OSDescription,
            processors = Environment.ProcessorCount,
            roslyn = typeof(Microsoft.CodeAnalysis.Compilation)
                .Assembly.GetName()
                .Version!.ToString(),
        };

    internal static void Write<T>(T value) => Console.WriteLine(JsonSerializer.Serialize(value));

    // Formats enabled messages but performs no console/disk/network I/O in the measured interval.
    private sealed class FormattingLogger : ILogger
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
        ) => _sink = formatter(state, exception);
    }
}

internal sealed record Measurement(double Nanoseconds, double AllocatedBytes);
