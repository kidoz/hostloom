using System.Diagnostics.CodeAnalysis;
using System.Diagnostics.Metrics;
using System.Reflection;
using System.Text;
using System.Text.Json;
using HostLoom.Logging;
using Xunit;

namespace Legacy.Unreadable
{
    /// <summary>A legacy masking attribute whose options cannot be read, as under Native AOT when
    /// nothing else calls its getters: the property exists, but reading it throws.</summary>
    [AttributeUsage(AttributeTargets.Property)]
    public sealed class LogMaskedAttribute : Attribute
    {
        public string Text => throw new NotSupportedException("The getter was not compiled.");

        public int ShowFirst { get; set; }
    }
}

namespace HostLoom.Tests
{
    public sealed class LoggingAotTests
    {
        [Fact]
        public void A_legacy_mask_whose_options_cannot_be_read_masks_its_member_whole()
        {
            using var failures = new DestructurerFailures();
            var destructurer = new Destructurer(new DestructuringOptions(), failures.Metrics);

            var json = Write(destructurer, new Contact { Name = "acme", Phone = "5551234" });

            // Only the member loses its reveal rule; the object and its other members survive.
            Assert.Equal("""{"Name":"acme","Phone":"***"}""", json);
            Assert.Equal(1, failures.Count);
        }

        [Fact]
        public void Under_native_code_a_type_without_readable_members_is_counted_once()
        {
            using var failures = new DestructurerFailures();
            var destructurer = new Destructurer(
                new DestructuringOptions(),
                failures.Metrics,
                nativeCode: true
            );

            Assert.Equal("{}", Write(destructurer, new Shipment()));
            Assert.Equal("{}", Write(destructurer, new Shipment()));

            Assert.Equal(1, failures.Count);
        }

        [Fact]
        public void Members_excluded_on_purpose_are_not_counted_as_missing_under_native_code()
        {
            using var failures = new DestructurerFailures();
            var destructurer = new Destructurer(
                new DestructuringOptions(),
                failures.Metrics,
                nativeCode: true
            );

            Assert.Equal("{}", Write(destructurer, new SealedInvoice { Token = "t-1" }));

            Assert.Equal(0, failures.Count);
        }

        [Fact]
        public void On_the_jit_a_type_without_members_is_not_counted()
        {
            using var failures = new DestructurerFailures();
            var destructurer = new Destructurer(
                new DestructuringOptions(),
                failures.Metrics,
                nativeCode: false
            );

            Assert.Equal("{}", Write(destructurer, new Shipment()));

            Assert.Equal(0, failures.Count);
        }

        [Theory]
        [InlineData(nameof(DestructuringOptions.Preserve))]
        [InlineData(nameof(DestructuringOptions.NotLogged))]
        [InlineData(nameof(DestructuringOptions.Mask))]
        public void Type_registrations_keep_the_members_destructuring_reads(string method)
        {
            // The annotation is what makes the trimmer and Native AOT keep the type argument's
            // properties and fields; without it, registering a type would not preserve it.
            var parameter = typeof(DestructuringOptions)
                .GetMethod(method)!
                .GetGenericArguments()
                .Single();

            var annotation = parameter.GetCustomAttribute<DynamicallyAccessedMembersAttribute>();

            Assert.NotNull(annotation);
            Assert.Equal(
                DynamicallyAccessedMemberTypes.PublicProperties
                    | DynamicallyAccessedMemberTypes.PublicFields,
                annotation.MemberTypes
            );
        }

        [Fact]
        public void Preserve_returns_the_options_for_chaining()
        {
            var options = new DestructuringOptions();

            Assert.Same(options, options.Preserve<Contact>());
        }

        private static string Write(Destructurer destructurer, object value) =>
            Encoding.UTF8.GetString(destructurer.Destructure(value, int.MaxValue));

        private sealed class Contact
        {
            public string? Name { get; set; }

            [Legacy.Unreadable.LogMasked(ShowFirst = 2)]
            public string? Phone { get; set; }
        }

        private sealed class Shipment;

        private sealed class SealedInvoice
        {
            [NotLogged]
            public string? Token { get; set; }
        }

        /// <summary>Counts destructurer failures reported by the one metrics instance it creates, so
        /// logging tests running in parallel cannot affect the count.</summary>
        private sealed class DestructurerFailures : IDisposable
        {
            private readonly MeterListener _listener = new();
            private int _constructingThread = -1;
            private long _count;

            public DestructurerFailures()
            {
                _listener.InstrumentPublished = (instrument, listener) =>
                {
                    if (
                        instrument.Meter.Name == LoggingMetrics.MeterName
                        && instrument.Name == "hostloom.logging.failures"
                        && Environment.CurrentManagedThreadId
                            == Volatile.Read(ref _constructingThread)
                    )
                    {
                        listener.EnableMeasurementEvents(instrument);
                    }
                };
                _listener.SetMeasurementEventCallback<long>(
                    (_, count, tags, _) =>
                    {
                        foreach (var tag in tags)
                        {
                            if (tag.Key == "component" && tag.Value is "destructurer")
                            {
                                Interlocked.Add(ref _count, count);
                            }
                        }
                    }
                );
                // Starting publishes every existing instrument; none is enabled while no metrics
                // instance of this helper is being constructed.
                _listener.Start();
                Volatile.Write(ref _constructingThread, Environment.CurrentManagedThreadId);
                Metrics = new LoggingMetrics(() => 0, () => true, () => "running");
                Volatile.Write(ref _constructingThread, -1);
            }

            public LoggingMetrics Metrics { get; }

            public long Count => Interlocked.Read(ref _count);

            public void Dispose()
            {
                _listener.Dispose();
                Metrics.Dispose();
            }
        }
    }
}
