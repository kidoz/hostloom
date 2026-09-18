namespace HostLoom.Mapping;

/// <summary>
/// Implements one explicit update from <typeparamref name="TSource"/> into an existing
/// <typeparamref name="TDestination"/> instance.
/// </summary>
/// <remarks>
/// <para>
/// An update map writes into the instance it is handed and never replaces it: the caller keeps the
/// identity it already holds, such as an entity tracked by a persistence context, and every member
/// the map does not assign keeps its previous value. That makes a partial update the normal case
/// rather than an omission, so the completeness analyzers that check a creation map do not apply
/// here. State in the class documentation which members an update deliberately leaves alone, and
/// choose collection behavior explicitly — replace, merge, or leave untouched — in the map body.
/// </para>
/// <para>
/// Both arguments are non-null by contract; an implementation rejects a null with
/// <see cref="ArgumentNullException"/> rather than treating it as "nothing to update". Like a
/// creation map, an update map is synchronous, deterministic, and free of database or network I/O.
/// A creation map and an update map for the same pair are distinct contracts and can be
/// registered side by side.
/// </para>
/// </remarks>
public interface IUpdateMapper<in TSource, in TDestination>
    where TSource : notnull
    where TDestination : class
{
    /// <summary>Writes <paramref name="source"/> into the supplied <paramref name="destination"/>.</summary>
    /// <param name="source">The non-null value to read from.</param>
    /// <param name="destination">The non-null instance to update in place.</param>
    void MapInto(TSource source, TDestination destination);
}
