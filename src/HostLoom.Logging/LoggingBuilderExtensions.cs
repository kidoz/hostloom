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
