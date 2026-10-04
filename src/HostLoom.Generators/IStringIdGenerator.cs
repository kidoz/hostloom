namespace HostLoom.Generators;

/// <summary>
/// An injectable seam for code that creates string identifiers, so the identifier format stays
/// an explicit dependency instead of an inline <c>Guid.NewGuid().ToString()</c> call.
/// </summary>
public interface IStringIdGenerator
{
    /// <summary>Creates a new string identifier in the generator's pinned format.</summary>
    string CreateStringId();
}
