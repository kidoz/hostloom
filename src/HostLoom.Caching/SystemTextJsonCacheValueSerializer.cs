using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;

namespace HostLoom.Caching;

/// <summary>
/// The default <see cref="ICacheValueSerializer"/>: <c>System.Text.Json</c> over a
/// <see cref="JsonSerializerOptions"/> whose <see cref="JsonSerializerOptions.TypeInfoResolver"/>
/// is set, typically a source-generated <see cref="JsonSerializerContext"/>.
/// </summary>
/// <remarks>
/// Contracts are resolved through <see cref="JsonSerializerOptions.GetTypeInfo"/> and written
/// through <see cref="Utf8JsonWriter"/>, which is the reflection-free path. The reflection-based
/// resolver is available only through <see cref="CreateReflectionBased"/>, which is annotated so
/// a trimmed or Native AOT publish warns at the call site.
/// </remarks>
public sealed class SystemTextJsonCacheValueSerializer : ICacheValueSerializer
{
    private readonly JsonSerializerOptions _options;

    /// <summary>Creates a serializer over <paramref name="options"/>.</summary>
    /// <exception cref="ArgumentException">
    /// <paramref name="options"/> has no <see cref="JsonSerializerOptions.TypeInfoResolver"/>.
    /// </exception>
    public SystemTextJsonCacheValueSerializer(JsonSerializerOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (options.TypeInfoResolver is null)
        {
            throw new ArgumentException(
                "JsonSerializerOptions.TypeInfoResolver must be set: a source-generated "
                    + "JsonSerializerContext for a trimmed or Native AOT publish, or "
                    + "DefaultJsonTypeInfoResolver through "
                    + $"{nameof(SystemTextJsonCacheValueSerializer)}.{nameof(CreateReflectionBased)} "
                    + "where reflection is acceptable.",
                nameof(options)
            );
        }

        options.MakeReadOnly();
        _options = options;
    }

    /// <summary>The options this serializer resolves contracts from.</summary>
    public JsonSerializerOptions Options => _options;

    /// <summary>
    /// A serializer whose contracts come from reflection. Not compatible with trimming or
    /// Native AOT; prefer a source-generated context there.
    /// </summary>
    /// <param name="options">
    /// Options to copy settings from; the copy receives a <see cref="DefaultJsonTypeInfoResolver"/>.
    /// When null, <see cref="ApplyPlatformProfile"/> defaults are used.
    /// </param>
    [RequiresUnreferencedCode(
        "Reflection-based JSON contracts are not compatible with trimming. Use a JsonSerializerContext."
    )]
    [RequiresDynamicCode(
        "Reflection-based JSON contracts are not compatible with Native AOT. Use a JsonSerializerContext."
    )]
    public static SystemTextJsonCacheValueSerializer CreateReflectionBased(
        JsonSerializerOptions? options = null
    )
    {
        var copy = options is null
            ? ApplyPlatformProfile(new JsonSerializerOptions())
            : new JsonSerializerOptions(options);
        copy.TypeInfoResolver = new DefaultJsonTypeInfoResolver();
        return new SystemTextJsonCacheValueSerializer(copy);
    }

    /// <summary>
    /// Applies the documented platform profile: camelCase names, nulls omitted, reference cycles
    /// ignored. Enums as strings are a per-context choice
    /// (<c>JsonSourceGenerationOptions.UseStringEnumConverter</c>) because the non-generic
    /// converter is not AOT-compatible.
    /// </summary>
    public static JsonSerializerOptions ApplyPlatformProfile(JsonSerializerOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.PropertyNamingPolicy = JsonNamingPolicy.CamelCase;
        options.DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull;
        options.ReferenceHandler = ReferenceHandler.IgnoreCycles;
        return options;
    }

    /// <inheritdoc />
    public void Serialize<T>(IBufferWriter<byte> destination, T value)
    {
        ArgumentNullException.ThrowIfNull(destination);
        using var writer = new Utf8JsonWriter(destination);
        JsonSerializer.Serialize(writer, value, GetTypeInfo<T>());
    }

    /// <inheritdoc />
    public T? Deserialize<T>(ReadOnlySpan<byte> payload) =>
        JsonSerializer.Deserialize(payload, GetTypeInfo<T>());

    private JsonTypeInfo<T> GetTypeInfo<T>() => (JsonTypeInfo<T>)_options.GetTypeInfo(typeof(T));
}
