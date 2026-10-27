using Confluent.Kafka;
using HostLoom.Transport.Kafka;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Xunit;

namespace HostLoom.Tests;

public sealed class KafkaConsumerShutdownTests
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(5);

    [Theory(Timeout = 30_000)]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Broker_disposal_releases_the_producer_while_a_handler_ignores_cancellation(
        bool request
    )
    {
        var consumer = Substitute.For<IConsumer<string, byte[]>>();
        consumer
            .Consume(Arg.Any<CancellationToken>())
            .Returns(
                new ConsumeResult<string, byte[]>
                {
                    Topic = "orders",
                    Partition = 0,
                    Offset = 0,
                    Message = new Message<string, byte[]>
                    {
                        Value = [],
                        Headers = new Headers
                        {
                            { "hostloom-correlation-id", "request-1"u8.ToArray() },
                            { "hostloom-reply-to", "orders.replies"u8.ToArray() },
                        },
                    },
                }
            );
        var entered = Signal();
        var release = Signal();
        var completed = Signal();
        var producer = Substitute.For<IProducer<string, byte[]>>();
        await using var broker = new KafkaRequestBroker(
            Options.Create(new KafkaOptions { AllowedReplyTopics = { "orders.replies" } }),
            null,
            producer,
            (_, _) => consumer
        );
        async ValueTask HandleAsync()
        {
            entered.TrySetResult();
            await release.Task;
            completed.TrySetResult();
        }
        var subscription = request
            ? await broker.ListenAsync(
                "orders",
                async (_, _) =>
                {
                    await HandleAsync();
                    return ReadOnlyMemory<byte>.Empty;
                },
                TestContext.Current.CancellationToken
            )
            : await broker.SubscribeAsync(
                "orders",
                "audit",
                (_, _) => HandleAsync(),
                TestContext.Current.CancellationToken
            );
        try
        {
            await entered.Task.WaitAsync(Bound, TestContext.Current.CancellationToken);
            await broker
                .DisposeAsync()
                .AsTask()
                .WaitAsync(Bound, TestContext.Current.CancellationToken);
            producer.Received(1).Dispose();
            consumer.Received(1).Close();
            consumer.Received(1).Dispose();
            Assert.False(completed.Task.IsCompleted);
        }
        finally
        {
            release.TrySetResult();
            await subscription
                .DisposeAsync()
                .AsTask()
                .WaitAsync(Bound, TestContext.Current.CancellationToken);
        }
        await completed.Task.WaitAsync(Bound, TestContext.Current.CancellationToken);
        consumer.DidNotReceive().Commit(Arg.Any<ConsumeResult<string, byte[]>>());
    }

    [Fact(Timeout = 30_000)]
    public async Task Broker_disposal_bounds_all_subscriptions_as_one_aggregate_wave()
    {
        var clock = new TestClock();
        var entered = Signal();
        var enteredCount = 0;
        var closedCount = 0;
        using var release = new ManualResetEventSlim();
        var consumers = Enumerable
            .Range(0, 2)
            .Select(_ =>
            {
                var consumer = Substitute.For<IConsumer<string, byte[]>>();
                consumer
                    .Consume(Arg.Any<CancellationToken>())
                    .Returns(
                        new ConsumeResult<string, byte[]>
                        {
                            Topic = "orders",
                            Partition = 0,
                            Offset = 0,
                            Message = new Message<string, byte[]> { Value = [] },
                        }
                    );
                consumer
                    .When(c => c.Close())
                    .Do(_ =>
                    {
                        Assert.True(release.Wait(TimeSpan.FromSeconds(15)));
                        Interlocked.Increment(ref closedCount);
                    });
                return consumer;
            })
            .ToArray();
        var consumerIndex = 0;
        var producer = Substitute.For<IProducer<string, byte[]>>();
        await using var broker = new KafkaRequestBroker(
            Options.Create(new KafkaOptions()),
            null,
            producer,
            (_, _) => consumers[consumerIndex++],
            clock
        );
        ValueTask HandleAsync()
        {
            if (Interlocked.Increment(ref enteredCount) == consumers.Length)
                entered.TrySetResult();
            return ValueTask.CompletedTask;
        }
        var subscriptions = new[]
        {
            await broker.SubscribeAsync(
                "orders",
                "audit",
                (_, _) => HandleAsync(),
                TestContext.Current.CancellationToken
            ),
            await broker.SubscribeAsync(
                "inventory",
                "audit",
                (_, _) => HandleAsync(),
                TestContext.Current.CancellationToken
            ),
        };
        try
        {
            await entered.Task.WaitAsync(Bound, TestContext.Current.CancellationToken);
            var disposing = broker.DisposeAsync().AsTask();
            await SchedulingTests.WaitUntilAsync(() => clock.PendingTimers >= consumers.Length);
            clock.Advance(ConsumerSubscription.DefaultShutdownTimeout);
            await disposing.WaitAsync(Bound, TestContext.Current.CancellationToken);
            Assert.Equal(0, closedCount);
        }
        finally
        {
            release.Set();
            foreach (var subscription in subscriptions)
                await subscription
                    .DisposeAsync()
                    .AsTask()
                    .WaitAsync(Bound, TestContext.Current.CancellationToken);
        }
    }

    [Theory(Timeout = 30_000)]
    [InlineData("handler")]
    [InlineData("handler_and_cancel")]
    [InlineData("cancel")]
    [InlineData("close")]
    public async Task Shutdown_bounds_blocking_code_and_defers_cleanup_without_racing_the_consumer(
        string blockAt
    )
    {
        var clock = new TestClock();
        var logger = new RecordingLogger<KafkaRequestBroker>();
        var entered = Signal();
        var cancellationObserved = Signal();
        var blocked = Signal();
        var disposed = Signal();
        var releaseHandler = Signal();
        using var release = new ManualResetEventSlim();
        var consumer = Substitute.For<IConsumer<string, byte[]>>();
        consumer.Consume(Arg.Any<CancellationToken>()).Returns(new ConsumeResult<string, byte[]>());
        void Block()
        {
            blocked.TrySetResult();
            Assert.True(
                release.Wait(TimeSpan.FromSeconds(15), TestContext.Current.CancellationToken)
            );
        }
        consumer
            .When(c => c.Close())
            .Do(_ =>
            {
                if (blockAt == "close")
                    Block();
            });
        consumer.When(c => c.Dispose()).Do(_ => disposed.TrySetResult());
        var subscription = ConsumerSubscription.Start(
            consumer,
            "orders",
            async (_, token) =>
            {
                using var registration = token.Register(() =>
                {
                    cancellationObserved.TrySetResult();
                    if (blockAt is "cancel" or "handler_and_cancel")
                        Block();
                });
                entered.TrySetResult();
                if (blockAt is "handler" or "handler_and_cancel")
                    Block();
                await releaseHandler.Task;
            },
            logger,
            commitOnSuccess: true,
            timeProvider: clock
        );
        try
        {
            await entered.Task.WaitAsync(Bound, TestContext.Current.CancellationToken);
            var disposing = subscription.DisposeAsync().AsTask();
            // The timeout is armed when cleanup is queued, before its worker necessarily starts.
            // Observe cancellation before advancing time and eventually releasing the handler.
            await cancellationObserved.Task.WaitAsync(Bound, TestContext.Current.CancellationToken);
            await blocked.Task.WaitAsync(Bound, TestContext.Current.CancellationToken);
            await SchedulingTests.WaitUntilAsync(() => clock.PendingTimers == 1);
            clock.Advance(ConsumerSubscription.DefaultShutdownTimeout);
            await disposing.WaitAsync(Bound, TestContext.Current.CancellationToken);
            Assert.False(disposed.Task.IsCompleted);
            Assert.Single(logger.Entries, entry => entry.Event.Id == 1431);
            if (blockAt != "close")
                consumer.DidNotReceive().Close();
        }
        finally
        {
            release.Set();
            releaseHandler.TrySetResult();
            await subscription
                .DisposeAsync()
                .AsTask()
                .WaitAsync(Bound, TestContext.Current.CancellationToken);
            await disposed.Task.WaitAsync(Bound, TestContext.Current.CancellationToken);
        }
        consumer.Received(1).Close();
        consumer.Received(1).Dispose();
        consumer.DidNotReceive().Commit(Arg.Any<ConsumeResult<string, byte[]>>());
    }

    [Theory(Timeout = 30_000)]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Shutdown_leaves_an_unfinished_record_uncommitted_and_cleans_up_once(
        bool failLate
    )
    {
        var consumer = Substitute.For<IConsumer<string, byte[]>>();
        var record = new ConsumeResult<string, byte[]>
        {
            Topic = "orders",
            Partition = 0,
            Offset = 0,
            Message = new Message<string, byte[]> { Value = [] },
        };
        consumer.Consume(Arg.Any<CancellationToken>()).Returns(record);
        var entered = Signal();
        var release = Signal();
        var completed = Signal();
        var subscription = ConsumerSubscription.Start(
            consumer,
            "orders",
            async (_, _) =>
            {
                entered.TrySetResult();
                await release.Task;
                completed.TrySetResult();
                if (failLate)
                    throw new InvalidOperationException("late handler failure");
            },
            NullLogger.Instance,
            commitOnSuccess: true
        );
        try
        {
            await entered.Task.WaitAsync(Bound, TestContext.Current.CancellationToken);
            var first = subscription.DisposeAsync().AsTask();
            var second = subscription.DisposeAsync().AsTask();
            await Task.WhenAll(first, second)
                .WaitAsync(Bound, TestContext.Current.CancellationToken);
            consumer.Received(1).Close();
            consumer.Received(1).Dispose();
            Assert.False(completed.Task.IsCompleted);
        }
        finally
        {
            release.TrySetResult();
            await subscription
                .DisposeAsync()
                .AsTask()
                .WaitAsync(Bound, TestContext.Current.CancellationToken);
        }
        await completed.Task.WaitAsync(Bound, TestContext.Current.CancellationToken);
        consumer.DidNotReceive().Commit(Arg.Any<ConsumeResult<string, byte[]>>());
        consumer.DidNotReceive().Seek(Arg.Any<TopicPartitionOffset>());
        consumer.Received(1).Consume(Arg.Any<CancellationToken>());
    }

    [Fact(Timeout = 30_000)]
    public async Task A_throwing_cancellation_callback_does_not_bypass_consumer_cleanup()
    {
        var logger = new RecordingLogger<KafkaRequestBroker>();
        var consumer = Substitute.For<IConsumer<string, byte[]>>();
        consumer.Consume(Arg.Any<CancellationToken>()).Returns(new ConsumeResult<string, byte[]>());
        var entered = Signal();
        var release = Signal();
        var subscription = ConsumerSubscription.Start(
            consumer,
            "orders",
            async (_, token) =>
            {
                using var callback = token.Register(() =>
                    throw new InvalidOperationException("cancel callback")
                );
                entered.TrySetResult();
                await release.Task;
            },
            logger
        );
        try
        {
            await entered.Task.WaitAsync(Bound, TestContext.Current.CancellationToken);
            await subscription
                .DisposeAsync()
                .AsTask()
                .WaitAsync(Bound, TestContext.Current.CancellationToken);
            consumer.Received(1).Close();
            consumer.Received(1).Dispose();
            Assert.Single(logger.Entries, entry => entry.Event.Id == 1432);
        }
        finally
        {
            release.TrySetResult();
            await subscription
                .DisposeAsync()
                .AsTask()
                .WaitAsync(Bound, TestContext.Current.CancellationToken);
        }
    }

    private static TaskCompletionSource Signal() =>
        new(TaskCreationOptions.RunContinuationsAsynchronously);
}
