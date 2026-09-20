using Confluent.Kafka;
using HostLoom.Transport.Kafka;
using Microsoft.Extensions.Options;
using NSubstitute;
using Xunit;

namespace HostLoom.Tests;

public sealed class KafkaLifecycleRegressionTests
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(10);

    [Theory(Timeout = 30_000)]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Request_deadline_includes_produce_and_assignment(bool completeProduce)
    {
        var clock = new TestClock();
        var consumer = Consumer();
        var producer = Substitute.For<IProducer<string, byte[]>>();
        var producing = Signal();
        var delivery = new TaskCompletionSource<DeliveryResult<string, byte[]>>(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        CancellationToken produceToken = default;
        producer
            .ProduceAsync(
                Arg.Any<string>(),
                Arg.Any<Message<string, byte[]>>(),
                Arg.Any<CancellationToken>()
            )
            .Returns(call =>
            {
                produceToken = call.Arg<CancellationToken>();
                producing.SetResult();
                return delivery.Task;
            });
        await using var broker = new KafkaRequestBroker(
            Options.Create(new KafkaOptions()),
            null,
            producer,
            (_, assigned) =>
            {
                clock.Advance(TimeSpan.FromSeconds(6));
                Assign(consumer, assigned);
                return consumer;
            },
            clock
        );
        var request = broker
            .RequestAsync(
                "orders",
                new byte[] { 1 },
                Guid.NewGuid(),
                Bound,
                TestContext.Current.CancellationToken
            )
            .AsTask();
        await producing.Task.WaitAsync(Bound, TestContext.Current.CancellationToken);
        if (completeProduce)
        {
            delivery.SetResult(new DeliveryResult<string, byte[]>());
        }
        clock.Advance(TimeSpan.FromSeconds(4));
        await Assert.ThrowsAsync<RequestTimeoutException>(() =>
            request.WaitAsync(Bound, TestContext.Current.CancellationToken)
        );
        Assert.True(produceToken.IsCancellationRequested);
        delivery.TrySetResult(new DeliveryResult<string, byte[]>());
    }

    [Fact(Timeout = 30_000)]
    public async Task Request_deadline_bounds_synchronous_consumer_initialization()
    {
        var clock = new TestClock();
        var entered = Signal();
        using var release = new ManualResetEventSlim();
        var consumer = Consumer();
        await using var broker = new KafkaRequestBroker(
            Options.Create(new KafkaOptions()),
            null,
            Substitute.For<IProducer<string, byte[]>>(),
            (_, assigned) =>
            {
                entered.SetResult();
                Assert.True(release.Wait(Bound, TestContext.Current.CancellationToken));
                Assign(consumer, assigned);
                return consumer;
            },
            clock
        );
        try
        {
            var request = broker
                .RequestAsync(
                    "orders",
                    new byte[] { 1 },
                    Guid.NewGuid(),
                    Bound,
                    TestContext.Current.CancellationToken
                )
                .AsTask();
            await entered.Task.WaitAsync(Bound, TestContext.Current.CancellationToken);
            clock.Advance(Bound);
            await Assert.ThrowsAsync<RequestTimeoutException>(() =>
                request.WaitAsync(Bound, TestContext.Current.CancellationToken)
            );
        }
        finally
        {
            release.Set();
        }
    }

    [Theory(Timeout = 30_000)]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Request_produce_wait_observes_caller_cancellation_and_shutdown(bool shutdown)
    {
        var consumer = Consumer();
        var producing = Signal();
        var delivery = new TaskCompletionSource<DeliveryResult<string, byte[]>>(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        var producer = Substitute.For<IProducer<string, byte[]>>();
        producer
            .ProduceAsync(
                Arg.Any<string>(),
                Arg.Any<Message<string, byte[]>>(),
                Arg.Any<CancellationToken>()
            )
            .Returns(_ =>
            {
                producing.SetResult();
                return delivery.Task;
            });
        await using var broker = new KafkaRequestBroker(
            Options.Create(new KafkaOptions()),
            null,
            producer,
            (_, assigned) =>
            {
                Assign(consumer, assigned);
                return consumer;
            }
        );
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(
            TestContext.Current.CancellationToken
        );
        var request = broker
            .RequestAsync(
                "orders",
                new byte[] { 1 },
                Guid.NewGuid(),
                Timeout.InfiniteTimeSpan,
                cancellation.Token
            )
            .AsTask();
        await producing.Task.WaitAsync(Bound, TestContext.Current.CancellationToken);
        if (shutdown)
        {
            await broker.DisposeAsync();
            await Assert.ThrowsAsync<ObjectDisposedException>(() =>
                request.WaitAsync(Bound, TestContext.Current.CancellationToken)
            );
        }
        else
        {
            await cancellation.CancelAsync();
            var failure = await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
                request.WaitAsync(Bound, TestContext.Current.CancellationToken)
            );
            Assert.Equal(cancellation.Token, failure.CancellationToken);
        }
        delivery.SetResult(new DeliveryResult<string, byte[]>());
    }

    [Theory(Timeout = 30_000)]
    [InlineData("request")]
    [InlineData("listen")]
    [InlineData("subscribe")]
    public async Task Shutdown_joins_initialization_and_disposes_the_unregistered_consumer(
        string operation
    )
    {
        var entered = Signal();
        using var release = new ManualResetEventSlim();
        var consumer = Consumer();
        await using var broker = new KafkaRequestBroker(
            Options.Create(new KafkaOptions()),
            null,
            Substitute.For<IProducer<string, byte[]>>(),
            (_, _) =>
            {
                entered.SetResult();
                Assert.True(release.Wait(Bound, TestContext.Current.CancellationToken));
                return consumer;
            }
        );
        var starting = Task.Run(
            () => StartAsync(broker, operation),
            TestContext.Current.CancellationToken
        );
        await entered.Task.WaitAsync(Bound, TestContext.Current.CancellationToken);
        Task disposing;
        try
        {
            disposing = broker.DisposeAsync().AsTask();
            Assert.False(disposing.IsCompleted);
            Assert.Same(disposing, broker.DisposeAsync().AsTask());
        }
        finally
        {
            release.Set();
        }
        await disposing.WaitAsync(Bound, TestContext.Current.CancellationToken);
        await Assert.ThrowsAsync<ObjectDisposedException>(() =>
            starting.WaitAsync(Bound, TestContext.Current.CancellationToken)
        );
        consumer.Received(1).Dispose();
        consumer.DidNotReceive().Subscribe(Arg.Any<string>());
    }

    [Theory(Timeout = 30_000)]
    [InlineData("request")]
    [InlineData("listen")]
    [InlineData("subscribe")]
    public async Task Failed_subscribe_releases_consumer_and_preserves_original_error(
        string operation
    )
    {
        var consumer = Consumer();
        var failure = new KafkaException(new Error(ErrorCode.Local_InvalidArg));
        consumer.When(c => c.Subscribe(Arg.Any<string>())).Do(_ => throw failure);
        consumer
            .When(c => c.Dispose())
            .Do(_ => throw new InvalidOperationException("cleanup failed"));
        await using var broker = new KafkaRequestBroker(
            Options.Create(new KafkaOptions()),
            null,
            Substitute.For<IProducer<string, byte[]>>(),
            (_, _) => consumer
        );
        Assert.Same(
            failure,
            await Assert.ThrowsAsync<KafkaException>(() =>
                StartAsync(broker, operation)
                    .WaitAsync(Bound, TestContext.Current.CancellationToken)
            )
        );
        await broker.DisposeAsync();
        consumer.Received(1).Dispose();
    }

    private static async Task StartAsync(KafkaRequestBroker broker, string operation)
    {
        switch (operation)
        {
            case "request":
                await broker.RequestAsync(
                    "orders",
                    new byte[] { 1 },
                    Guid.NewGuid(),
                    Bound,
                    TestContext.Current.CancellationToken
                );
                break;
            case "listen":
                await broker.ListenAsync(
                    "orders",
                    (_, _) => ValueTask.FromResult<ReadOnlyMemory<byte>>(new byte[] { 1 }),
                    TestContext.Current.CancellationToken
                );
                break;
            default:
                await broker.SubscribeAsync(
                    "orders",
                    "audit",
                    (_, _) => ValueTask.CompletedTask,
                    TestContext.Current.CancellationToken
                );
                break;
        }
    }

    private static TaskCompletionSource Signal() =>
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    private static IConsumer<string, byte[]> Consumer()
    {
        var consumer = Substitute.For<IConsumer<string, byte[]>>();
        consumer
            .Consume(Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                var token = call.Arg<CancellationToken>();
                token.WaitHandle.WaitOne();
                throw new OperationCanceledException(token);
            });
        consumer
            .QueryWatermarkOffsets(Arg.Any<TopicPartition>(), Arg.Any<TimeSpan>())
            .Returns(new WatermarkOffsets(0, 0));
        return consumer;
    }

    private static void Assign(
        IConsumer<string, byte[]> consumer,
        PartitionsAssignedHandler? assigned
    ) => assigned?.Invoke(consumer, [new TopicPartition("hostloom.responses", 0)]).ToArray();
}
