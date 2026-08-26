using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;

namespace HostLoom.Logging;

public static class LoggingBuilderExtensions
{
    /// <summary>
    /// Registers the provider behind <see cref="ILoggingBuilder"/>, so it composes with the standard
    /// filter configuration and can run alongside an existing provider during a migration.
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
        return Register(builder, sink, options, formatter);
    }

    /// <summary>
    /// Registers the provider with options bound from configuration — typically
    /// <c>configuration.GetSection("HostLoom:Logging")</c>. The optional callback applies after
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

        var options = new HostLoomLoggerOptions();
        BindOptions(options, configuration);
        configure?.Invoke(options);
        return Register(builder, sink, options, formatter);
    }

    internal static void BindOptions(HostLoomLoggerOptions options, IConfiguration configuration) =>
        // Strict on purpose: a typo in a cap or policy name should fail startup loudly rather
        // than silently leave the default in place.
        configuration.Bind(options, binder => binder.ErrorOnUnknownConfiguration = true);

    private static ILoggingBuilder Register(
        ILoggingBuilder builder,
        ILogSink sink,
        HostLoomLoggerOptions options,
        ILogFormatter? formatter
    )
    {
        // CA2000: ownership transfers to the container, which disposes registered singletons and so
        // drains the pipeline at shutdown.
#pragma warning disable CA2000
        builder.Services.TryAddEnumerable(
            ServiceDescriptor.Singleton<ILoggerProvider>(
                new HostLoomLoggerProvider(formatter ?? new JsonLogFormatter(), sink, options)
            )
        );
#pragma warning restore CA2000
        return builder;
    }
}
