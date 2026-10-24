using System.Collections.Concurrent;
using System.Text;
using Confluent.Kafka;
using HostLoom.Transport.Kafka;
using Microsoft.Extensions.Options;
using NSubstitute;
using Xunit;

namespace HostLoom.Tests;

public sealed class KafkaFrameLimitTests
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(5);

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Oversized_deliveries_are_committed_as_poison_and_the_next_record_is_handled(
        bool eventFrame
    )
    {
        using var records = new BlockingCollection<ConsumeResult<string, byte[]>>();
        records.Add(Record(0, new byte[33]), TestContext.Current.CancellationToken);
        records.Add(Record(1, new byte[32]), TestContext.Current.CancellationToken);
        var consumer = Consumer(records);
        var committed = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        var commits = 0;
        consumer
            .When(c => c.Commit(Arg.Any<ConsumeResult<string, byte[]>>()))
            .Do(_ =>
            {
                if (Interlocked.Increment(ref commits) == 2)
                    committed.SetResult();
            });
        await using var broker = new KafkaRequestBroker(
            Options.Create(new KafkaOptions { AllowedReplyTopics = { "orders.replies" } }),
            null,
            Substitute.For<IProducer<string, byte[]>>(),
            (_, _) => consumer,
            messagingOptions: Options.Create(new HostLoomOptions { MaxFrameBytes = 32 })
        );
        var handled = 0;
        await using var subscription = eventFrame
            ? await broker.SubscribeAsync(
                "orders",
                "audit",
                (_, _) =>
                {
                    handled++;
                    return ValueTask.CompletedTask;
                },
                TestContext.Current.CancellationToken
            )
            : await broker.ListenAsync(
                "orders",
                (_, _) =>
                {
                    handled++;
                    return ValueTask.FromResult(ReadOnlyMemory<byte>.Empty);
                },
                TestContext.Current.CancellationToken
            );
        await committed.Task.WaitAsync(Bound, TestContext.Current.CancellationToken);
        Assert.Equal(1, handled);
        consumer.DidNotReceive().Seek(Arg.Any<TopicPartitionOffset>());
    }

    [Theory]
    [InlineData(32)]
    [InlineData(33)]
    public async Task Correlated_reply_frames_are_bounded_without_stopping_the_reply_consumer(
        int size
    )
    {
        using var records = new BlockingCollection<ConsumeResult<string, byte[]>>();
        var consumer = Consumer(records);
        var producer = Substitute.For<IProducer<string, byte[]>>();
        var replySize = size;
        producer
            .ProduceAsync(
                Arg.Any<string>(),
                Arg.Any<Message<string, byte[]>>(),
                Arg.Any<CancellationToken>()
            )
            .Returns(call =>
            {
                var sent = call.Arg<Message<string, byte[]>>();
                records.Add(
                    Record(
                        0,
                        new byte[replySize],
                        sent.Headers.GetLastBytes("hostloom-correlation-id")
                    )
                );
                return Task.FromResult(new DeliveryResult<string, byte[]>());
            });
        await using var broker = new KafkaRequestBroker(
            Options.Create(new KafkaOptions()),
            null,
            producer,
            (_, assigned) =>
            {
                assigned?.Invoke(consumer, [new TopicPartition("hostloom.responses", 0)]).ToArray();
                return consumer;
            },
            messagingOptions: Options.Create(new HostLoomOptions { MaxFrameBytes = 32 })
        );
        async Task<ReadOnlyMemory<byte>> AskAsync() =>
            await broker.RequestAsync(
                "orders",
                new byte[1],
                Guid.NewGuid(),
                Bound,
                TestContext.Current.CancellationToken
            );
        if (size > 32)
            await Assert.ThrowsAsync<MalformedEnvelopeException>(async () => await AskAsync());
        else
            Assert.Equal(32, (await AskAsync()).Length);
        replySize = 32;
        Assert.Equal(32, (await AskAsync()).Length);
        Assert.Equal(0, broker.PendingRequestCount);
    }

    private static IConsumer<string, byte[]> Consumer(
        BlockingCollection<ConsumeResult<string, byte[]>> records
    )
    {
        var consumer = Substitute.For<IConsumer<string, byte[]>>();
        consumer
            .Consume(Arg.Any<CancellationToken>())
            .Returns(call => records.Take(call.Arg<CancellationToken>()));
        consumer
            .QueryWatermarkOffsets(Arg.Any<TopicPartition>(), Arg.Any<TimeSpan>())
            .Returns(new WatermarkOffsets(0, 0));
        return consumer;
    }

    private static ConsumeResult<string, byte[]> Record(
        long offset,
        byte[] value,
        byte[]? correlation = null
    ) =>
        new()
        {
            Topic = "orders",
            Partition = 0,
            Offset = offset,
            Message = new Message<string, byte[]>
            {
                Value = value,
                Headers = new Headers
                {
                    {
                        "hostloom-correlation-id",
                        correlation ?? Encoding.UTF8.GetBytes(Guid.NewGuid().ToString("N"))
                    },
                    { "hostloom-reply-to", "orders.replies"u8.ToArray() },
                },
            },
        };
}
