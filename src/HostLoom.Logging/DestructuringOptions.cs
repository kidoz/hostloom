namespace HostLoom.Logging;

/// <summary>How one protected member is treated. An excluded member is never read at all.</summary>
internal sealed record MemberRule(bool Excluded, MaskRule? Mask)
{
    public static readonly MemberRule NotLogged = new(true, null);
}

/// <summary>Deterministic masking: reveal that many leading/trailing characters around the text.</summary>
internal sealed record MaskRule(string Text, int ShowFirst, int ShowLast);

/// <summary>
/// Caps and protection policy for <c>{@...}</c> destructuring. Every cap produces valid JSON with
/// an explicit non-sensitive truncation sentinel instead of unbounded output. The per-type policy
/// is registration-time configuration: register everything before the provider is built.
/// </summary>
public sealed class DestructuringOptions
{
    private readonly Dictionary<Type, Dictionary<string, MemberRule>> _policies = [];

    /// <summary>Nesting levels below the hole itself; deeper complex values become "…".</summary>
    public int MaxDepth { get; set; } = 5;

    /// <summary>Items serialized per collection; the cut is marked with a trailing "…" element.</summary>
    public int MaxCollectionItems { get; set; } = 32;

    /// <summary>Members serialized per object or dictionary; the cut is marked with a "…" member.</summary>
    public int MaxObjectMembers { get; set; } = 64;

    /// <summary>Characters kept per string value; longer strings are truncated with a "…" suffix.</summary>
    public int MaxStringLength { get; set; } = 4096;

    /// <summary>
    /// Encoded destructured bytes one record may carry across all its <c>{@...}</c> holes,
    /// enforced at element boundaries. Holes past the budget degrade to a "…" field.
    /// </summary>
    public int MaxEncodedBytesPerRecord { get; set; } = 64 * 1024;

    /// <summary>
    /// Recognize legacy attributes named <c>NotLoggedAttribute</c> / <c>LogMaskedAttribute</c>
    /// from any namespace (Destructurama.Attributed in particular) by name, so annotated DTOs
    /// keep their protection during a staged migration without a package dependency.
    /// </summary>
    public bool MapLegacyAttributes { get; set; } = true;

    /// <summary>Excludes members of <typeparamref name="T"/> (and derived types) that cannot be
    /// annotated. Excluded members are never read.</summary>
    public DestructuringOptions NotLogged<T>(params string[] members)
    {
        ArgumentNullException.ThrowIfNull(members);
        var rules = RulesFor(typeof(T));
        foreach (var member in members)
        {
            rules[member] = MemberRule.NotLogged;
        }

        return this;
    }

    /// <summary>Masks one member of <typeparamref name="T"/> (and derived types) like
    /// <see cref="LogMaskedAttribute"/> would.</summary>
    public DestructuringOptions Mask<T>(
        string member,
        string text = "***",
        int showFirst = 0,
        int showLast = 0
    )
    {
        ArgumentNullException.ThrowIfNull(member);
        ArgumentNullException.ThrowIfNull(text);
        RulesFor(typeof(T))[member] = new MemberRule(
            false,
            new MaskRule(text, showFirst, showLast)
        );
        return this;
    }

    internal MemberRule? RuleFor(Type type, string member)
    {
        MemberRule? found = null;
        foreach (var (registered, rules) in _policies)
        {
            if (registered.IsAssignableFrom(type) && rules.TryGetValue(member, out var rule))
            {
                if (rule.Excluded)
                {
                    return rule;
                }

                found ??= rule;
            }
        }

        return found;
    }

    private Dictionary<string, MemberRule> RulesFor(Type type)
    {
        if (!_policies.TryGetValue(type, out var rules))
        {
            rules = new Dictionary<string, MemberRule>(StringComparer.Ordinal);
            _policies[type] = rules;
        }

        return rules;
    }
}
