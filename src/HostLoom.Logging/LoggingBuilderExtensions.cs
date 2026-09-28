using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace HostLoom.Logging;

public static class LoggingBuilderExtensions
{
    /// <summary>
    /// Registers the provider with options, the formatter among them, bound from configuration —
    /// typically <c>configuration.GetSection("HostLoom:Logging")</c> — and writing to standard
    /// output through a <see cref="StreamLogSink.Console"/> sink that each container opens when
    /// it first resolves logging and closes at disposal. The optional callback applies after
    /// configuration.
    /// </summary>
    public static ILoggingBuilder AddHostLoomLogging(
        this ILoggingBuilder builder,
        IConfiguration configuration,
        Action<HostLoomLoggerOptions>? configure = null
    ) => AddHostLoomLogging(builder, _ => StreamLogSink.Console(), configuration, configure);

    /// <summary>
    /// Registers the provider behind <see cref="ILoggingBuilder"/>, so it composes with the standard
    /// filter configuration and can run alongside an existing provider during a migration. A
    /// <paramref name="formatter"/> passed here takes precedence over
    /// <see cref="HostLoomLoggerOptions.Formatter"/>. Registering the provider a second time
    /// throws; call <c>ClearProviders()</c> first to replace the registration.
    /// </summary>
    public static ILoggingBuilder AddHostLoomLogging(
        this ILoggingBuilder builder,
        ILogSink sink,
        Action<HostLoomLoggerOptions>? configure = null,
        ILogFormatter? formatter = null
    )
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(sink);

        var options = new HostLoomLoggerOptions();
        configure?.Invoke(options);
        return Register(builder, _ => sink, options, formatter);
    }

    /// <summary>
    /// Registers the provider with a sink that <paramref name="sink"/> creates when a container
    /// first resolves logging, so each container built from the service collection writes to,
    /// and at disposal releases, a sink of its own. The overload taking an
    /// <see cref="ILogSink"/> hands that one instance to every such container.
    /// </summary>
    public static ILoggingBuilder AddHostLoomLogging(
        this ILoggingBuilder builder,
        Func<IServiceProvider, ILogSink> sink,
        Action<HostLoomLoggerOptions>? configure = null,
        ILogFormatter? formatter = null
    )
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(sink);

        var options = new HostLoomLoggerOptions();
        configure?.Invoke(options);
        return Register(builder, sink, options, formatter);
    }

    /// <summary>
    /// Registers the provider with options bound from configuration — typically
    /// <c>configuration.GetSection("HostLoom:Logging")</c> — through
    /// <see cref="HostLoomLoggerOptions.Bind"/>. The optional callback applies after
    /// configuration, and invalid or unknown values fail here, at host startup, not at first
    /// log. Level filtering stays standard MEL <c>Logging</c> configuration, which runs before
    /// this provider.
    /// </summary>
    public static ILoggingBuilder AddHostLoomLogging(
        this ILoggingBuilder builder,
        ILogSink sink,
        IConfiguration configuration,
        Action<HostLoomLoggerOptions>? configure = null,
        ILogFormatter? formatter = null
    )
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(sink);
        ArgumentNullException.ThrowIfNull(configuration);

        var options = new HostLoomLoggerOptions().Bind(configuration);
        configure?.Invoke(options);
        return Register(builder, _ => sink, options, formatter);
    }

    /// <summary>
    /// Registers the provider with options bound from configuration, as the overload taking an
    /// <see cref="ILogSink"/> does, and with a sink that <paramref name="sink"/> creates for each
    /// container when it first resolves logging.
    /// </summary>
    public static ILoggingBuilder AddHostLoomLogging(
        this ILoggingBuilder builder,
        Func<IServiceProvider, ILogSink> sink,
        IConfiguration configuration,
        Action<HostLoomLoggerOptions>? configure = null,
        ILogFormatter? formatter = null
    )
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(sink);
        ArgumentNullException.ThrowIfNull(configuration);

        var options = new HostLoomLoggerOptions().Bind(configuration);
        configure?.Invoke(options);
        return Register(builder, sink, options, formatter);
    }

    private static ILoggingBuilder Register(
        ILoggingBuilder builder,
        Func<IServiceProvider, ILogSink> sink,
        HostLoomLoggerOptions options,
        ILogFormatter? formatter
    )
    {
        // Fail at registration rather than when logging is first resolved: a bad value set in
        // configuration or in the callback then surfaces at the line that registered it.
        LogPipeline.Validate(options);

        // The logger factory runs every registered provider, so adding a second registration would
        // run a second queue and sink, and deduplicating it would drop its sink, formatter, and
        // options without a word. Neither is what a second call intends.
        if (builder.Services.Any(IsHostLoomProvider))
        {
            throw new InvalidOperationException(
                "HostLoom logging is already registered with this service collection. Register it "
                    + "once, or call ClearProviders() before registering it again."
            );
        }

        // A factory, not an instance: the container disposes only the singletons it creates, and
        // the logger factory never disposes providers it receives from the container. Created
        // here, the provider is disposed with the container, which drains the pipeline and
        // flushes and disposes the sink at shutdown; the writer thread also starts only when
        // logging is first resolved.
        var registration = new ProviderRegistration(
            sink,
            options,
            formatter ?? LogFormatterNames.Create(options, LogFormatterNames.Json)
        );
        builder.Services.Add(
            ServiceDescriptor.Singleton<ILoggerProvider, HostLoomLoggerProvider>(
                registration.Create
            )
        );
        return builder;
    }

    private static bool IsHostLoomProvider(ServiceDescriptor descriptor) =>
        descriptor.ServiceType == typeof(ILoggerProvider)
        && !descriptor.IsKeyedService
        && (
            descriptor.ImplementationFactory?.Target is ProviderRegistration
            || descriptor.ImplementationType == typeof(HostLoomLoggerProvider)
            || descriptor.ImplementationInstance is HostLoomLoggerProvider
        );

    /// <summary>One registration's provider factory; its type marks the descriptor as HostLoom's.</summary>
    private sealed class ProviderRegistration(
        Func<IServiceProvider, ILogSink> sink,
        HostLoomLoggerOptions options,
        ILogFormatter formatter
    )
    {
        public HostLoomLoggerProvider Create(IServiceProvider services) =>
            new(
                formatter,
                sink(services)
                    ?? throw new InvalidOperationException(
                        "The sink factory registered with AddHostLoomLogging returned null."
                    ),
                options
            );
    }
}
