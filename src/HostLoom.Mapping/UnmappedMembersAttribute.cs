namespace HostLoom.Mapping;

/// <summary>
/// Declares the destination members a map deliberately leaves unset, so completeness analysis
/// reports the ones it does not.
/// </summary>
/// <remarks>
/// Omitting a member is often correct — a transfer with no payment provider has no card mask —
/// but "this map is incomplete on purpose" is not enough on its own, because it would also excuse
/// every member added to the contract later. Naming each member keeps that from happening: a new
/// destination member is neither assigned nor listed here, so it is still reported. Use
/// <c>nameof</c> rather than string literals so a rename stays a compile-time concern.
/// </remarks>
/// <example>
/// <code>
/// [UnmappedMembers(nameof(CfaTransfer.CardMask), nameof(CfaTransfer.ProviderId))]
/// public sealed class TransferModelToCfaTransferMapper : IMapper&lt;TransferModel, CfaTransfer&gt;
/// </code>
/// </example>
[AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = false)]
public sealed class UnmappedMembersAttribute : Attribute
{
    /// <summary>Declares the destination members this map does not assign.</summary>
    public UnmappedMembersAttribute(params string[] members) =>
        Members = members ?? throw new ArgumentNullException(nameof(members));

    /// <summary>The destination member names this map deliberately leaves unset.</summary>
    public IReadOnlyList<string> Members { get; }
}
