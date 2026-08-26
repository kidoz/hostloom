namespace HostLoom.Logging;

/// <summary>
/// Excludes a property or field from destructured log output completely, at every nesting level,
/// including on inherited members. The member is never read: no null, mask, or placeholder is
/// emitted in its place, and a throwing getter can leak nothing. Wins over
/// <see cref="LogMaskedAttribute"/> when both are present.
/// </summary>
[AttributeUsage(
    AttributeTargets.Property | AttributeTargets.Field,
    AllowMultiple = false,
    Inherited = true
)]
public sealed class NotLoggedAttribute : Attribute { }

/// <summary>
/// Replaces a property or field value with <see cref="Text"/> ("***" by default) in destructured
/// log output. <see cref="ShowFirst"/> and <see cref="ShowLast"/> deterministically reveal that
/// many leading and trailing characters of the value's invariant string representation around the
/// mask; with both at zero the member is never read at all.
/// </summary>
[AttributeUsage(
    AttributeTargets.Property | AttributeTargets.Field,
    AllowMultiple = false,
    Inherited = true
)]
public sealed class LogMaskedAttribute : Attribute
{
    public string Text { get; set; } = "***";

    public int ShowFirst { get; set; }

    public int ShowLast { get; set; }
}
