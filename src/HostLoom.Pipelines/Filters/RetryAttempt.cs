namespace HostLoom.Pipelines;

/// <summary>
/// Context payload recording which retry is in progress. Absent on the first attempt, so
/// <c>TryGetPayload&lt;RetryAttempt&gt;</c> returning false means the invocation is the original one.
/// </summary>
public sealed record RetryAttempt(int Number);
