using HostLoom.Pipelines;
using Xunit;

namespace HostLoom.Tests;

public sealed class PipeContextPayloadTests
{
    [Fact]
    public void A_context_payload_is_updated_in_place_and_remains_discoverable()
    {
        var context = new StampedContext();
        Assert.Same(context, context.GetOrAddPayload<IStamp>(() => new Stamp()));
        Assert.True(context.HasPayload(typeof(IStamp)));

        IStamp updated = context.AddOrUpdatePayload<IStamp>(
            () => throw new InvalidOperationException("The existing payload must be updated."),
            existing =>
            {
                Assert.Same(context, existing);
                existing.Count++;
                return existing;
            }
        );

        Assert.Same(context, updated);
        Assert.True(context.TryGetPayload<IStamp>(out var fetched));
        Assert.Same(updated, fetched);
        Assert.Equal(1, fetched!.Count);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void A_context_payload_cannot_be_replaced_or_removed(bool returnNull)
    {
        var context = new StampedContext();
        var updates = 0;

        Assert.Throws<InvalidOperationException>(() =>
            context.AddOrUpdatePayload<IStamp>(
                () => new Stamp(),
                existing =>
                {
                    updates++;
                    Assert.Same(context, existing);
                    return returnNull ? null! : new Stamp();
                }
            )
        );

        Assert.Equal(1, updates);
        Assert.True(context.TryGetPayload<IStamp>(out var fetched));
        Assert.Same(context, fetched);
    }

    [Fact]
    public void Context_payload_updates_are_serialized()
    {
        var context = new StampedContext();
        Parallel.For(
            0,
            1000,
            _ =>
                context.AddOrUpdatePayload<IStamp>(
                    () => throw new InvalidOperationException("A context payload already exists."),
                    existing =>
                    {
                        existing.Count++;
                        return existing;
                    }
                )
        );

        Assert.Equal(1000, context.Count);
    }

    [Fact]
    public void Dictionary_payloads_can_still_be_replaced()
    {
        var context = new PipeContext();
        var first = context.GetOrAddPayload<IStamp>(() => new Stamp());
        var replacement = context.AddOrUpdatePayload<IStamp>(
            () => throw new InvalidOperationException("A payload already exists."),
            existing => new Stamp { Count = existing.Count + 1 }
        );

        Assert.NotSame(first, replacement);
        Assert.True(context.TryGetPayload<IStamp>(out var fetched));
        Assert.Same(replacement, fetched);
        Assert.Equal(1, fetched!.Count);
    }

    private interface IStamp
    {
        int Count { get; set; }
    }

    private sealed class Stamp : IStamp
    {
        public int Count { get; set; }
    }

    private sealed class StampedContext : PipeContext, IStamp
    {
        public int Count { get; set; }
    }
}
