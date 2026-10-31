using HostLoom.Pipelines;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace HostLoom;

public static class KillSwitchRegistration
{
    /// <summary>
    /// Pauses one receive destination when its failure ratio trips. Call this from
    /// <c>ConfigureReceivePipeline</c>, in the order the filter should run.
    /// </summary>
    /// <remarks>
    /// The pipeline is composed once, with the application provider available. The filter shares
    /// the process <see cref="ReceiveAdmission"/> the transports consult.
    /// </remarks>
    public static PipeBuilder<ReceiveContext> UseKillSwitch(
        this PipeBuilder<ReceiveContext> builder,
        KillSwitchPolicy policy,
        TimeProvider? timeProvider = null
    )
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(policy);
        var services =
            ReceivePipelineServices.Current
            ?? throw new InvalidOperationException(
                "UseKillSwitch can only be called while the receive pipeline is being composed."
            );
        var admission = services.GetRequiredService<ReceiveAdmission>();
        var clock = timeProvider ?? services.GetService<TimeProvider>() ?? TimeProvider.System;
        var logger = services
            .GetService<ILoggerFactory>()
            ?.CreateLogger("HostLoom.KillSwitch")
            ?? Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance;
        return builder.Use(new KillSwitchFilter(policy, admission, clock, logger));
    }
}

/// <summary>The provider composing the receive pipeline, so filters can resolve process services.</summary>
internal static class ReceivePipelineServices
{
    private static readonly AsyncLocal<IServiceProvider?> Ambient = new();

    internal static IServiceProvider? Current => Ambient.Value;

    internal static IDisposable Begin(IServiceProvider services)
    {
        var previous = Ambient.Value;
        Ambient.Value = services;
        return new Pop(previous);
    }

    private sealed class Pop(IServiceProvider? previous) : IDisposable
    {
        public void Dispose() => Ambient.Value = previous;
    }
}
