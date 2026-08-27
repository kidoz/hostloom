using System.Text;
using System.Text.Json;
using HostLoom.Logging;
using Microsoft.Extensions.Logging;
using Xunit;

// CA1873: the boxing standard ILogger path is exactly what every test here exercises.
#pragma warning disable CA1873

namespace Destructurama.Attributed
{
    /// <summary>Stand-in for the legacy package's attribute: HostLoom recognizes it purely by
    /// type name, so annotated platform DTOs keep their protection without the dependency.</summary>
    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
    public sealed class NotLoggedAttribute : Attribute { }
}

namespace HostLoom.Tests
{
    public sealed class LoggingDestructuringTests
    {
        [Fact]
        public async Task A_destructured_object_becomes_nested_typed_json()
        {
            var (root, _) = await LogAsync(logger =>
                logger.LogInformation("got {@Order}", new Order())
            );

            var order = root.GetProperty("Order");
            Assert.Equal(JsonValueKind.Object, order.ValueKind);
            Assert.Equal(42, order.GetProperty("Id").GetInt32());
            Assert.Equal("ada", order.GetProperty("Customer").GetString());
            Assert.True(order.GetProperty("Express").GetBoolean());
            Assert.Equal(19.99m, order.GetProperty("Total").GetDecimal());
            Assert.Equal(3, order.GetProperty("Lines").GetProperty("count").GetInt32());
        }

        [Fact]
        public async Task NotLogged_members_are_omitted_at_every_level_including_inherited()
        {
            var account = new Account
            {
                Owner = "ada",
                Password = "hunter2",
                Nested = new Account { Password = "hunter3" },
            };
            var (root, line) = await LogAsync(logger =>
                logger.LogInformation("login {@Account}", account)
            );

            var logged = root.GetProperty("Account");
            Assert.Equal("ada", logged.GetProperty("Owner").GetString());
            // Declared on the base class, absent on the derived instance — and absent means
            // absent: no null, no mask, no placeholder, at any nesting level.
            Assert.False(logged.TryGetProperty("Password", out _));
            Assert.False(logged.GetProperty("Nested").TryGetProperty("Password", out _));
            Assert.DoesNotContain("hunter", line, StringComparison.Ordinal);
        }

        [Fact]
        public async Task LogMasked_replaces_or_deterministically_reveals()
        {
            var card = new Payment { Token = "secret-token", Card = "1234567890123456" };
            var (root, line) = await LogAsync(logger =>
                logger.LogInformation("pay {@Payment}", card)
            );

            var logged = root.GetProperty("Payment");
            Assert.Equal("***", logged.GetProperty("Token").GetString());
            Assert.Equal("12***56", logged.GetProperty("Card").GetString());
            Assert.DoesNotContain("secret-token", line, StringComparison.Ordinal);
            Assert.DoesNotContain("1234567890123456", line, StringComparison.Ordinal);
        }

        [Fact]
        public async Task NotLogged_wins_when_both_attributes_are_present()
        {
            var (root, line) = await LogAsync(logger =>
                logger.LogInformation("both {@Value}", new Contested { Secret = "tricky" })
            );

            Assert.False(root.GetProperty("Value").TryGetProperty("Secret", out _));
            Assert.DoesNotContain("tricky", line, StringComparison.Ordinal);
            Assert.DoesNotContain(
                "***",
                root.GetProperty("Value").GetRawText(),
                StringComparison.Ordinal
            );
        }

        [Fact]
        public async Task A_throwing_getter_yields_the_sentinel_and_never_tostring()
        {
            var (root, line) = await LogAsync(logger =>
                logger.LogInformation("broken {@Thing}", new Volatile())
            );

            var thing = root.GetProperty("Thing");
            Assert.Equal("ok", thing.GetProperty("Fine").GetString());
            Assert.Equal("[DestructuringFailed]", thing.GetProperty("Broken").GetString());
            // Neither the exception message nor ToString may leak into the output.
            Assert.DoesNotContain("secret", line, StringComparison.Ordinal);
        }

        [Fact]
        public async Task Cycles_are_cut_with_a_sentinel()
        {
            var node = new Node { Name = "a" };
            node.Next = node;
            var (root, _) = await LogAsync(logger => logger.LogInformation("looped {@Node}", node));

            Assert.Equal("[Cycle]", root.GetProperty("Node").GetProperty("Next").GetString());
        }

        [Fact]
        public async Task Caps_truncate_deterministically_with_valid_json()
        {
            var value = new
            {
                Deep = new { Level2 = new { Level3 = 1 } },
                Many = new[] { 1, 2, 3, 4 },
                Text = "abcdef",
            };
            var (root, _) = await LogAsync(
                logger => logger.LogInformation("capped {@Value}", value),
                options =>
                {
                    options.Destructuring.MaxDepth = 2;
                    options.Destructuring.MaxCollectionItems = 2;
                    options.Destructuring.MaxStringLength = 3;
                }
            );

            var logged = root.GetProperty("Value");
            // Depth 2: the hole's object is depth 0, Deep's object is depth 1, Level2's would be
            // depth 2 — replaced by the sentinel while everything shallower survives.
            Assert.Equal("…", logged.GetProperty("Deep").GetProperty("Level2").GetString());
            var many = logged.GetProperty("Many").EnumerateArray().ToArray();
            Assert.Equal(3, many.Length);
            Assert.Equal(1, many[0].GetInt32());
            Assert.Equal(2, many[1].GetInt32());
            Assert.Equal("…", many[2].GetString());
            Assert.Equal("abc…", logged.GetProperty("Text").GetString());
        }

        [Fact]
        public async Task Dictionaries_enums_bytes_and_dates_have_documented_shapes()
        {
            var value = new
            {
                Map = new Dictionary<string, int> { ["a"] = 1, ["b"] = 2 },
                Day = DayOfWeek.Friday,
                Blob = new byte[] { 1, 2, 3 },
                When = new DateTimeOffset(2026, 8, 26, 10, 0, 0, TimeSpan.Zero),
            };
            var (root, _) = await LogAsync(logger =>
                logger.LogInformation("shapes {@Value}", value)
            );

            var logged = root.GetProperty("Value");
            Assert.Equal(1, logged.GetProperty("Map").GetProperty("a").GetInt32());
            Assert.Equal("Friday", logged.GetProperty("Day").GetString());
            Assert.Equal("AQID", logged.GetProperty("Blob").GetString());
            Assert.Equal(
                "2026-08-26T10:00:00.0000000+00:00",
                logged.GetProperty("When").GetString()
            );
        }

        [Fact]
        public async Task The_per_type_policy_protects_unannotatable_types()
        {
            var dto = new ThirdPartyDto
            {
                Name = "ada",
                ApiKey = "k-123456",
                Card = "1234567890123456",
            };
            var (root, line) = await LogAsync(
                logger => logger.LogInformation("3p {@Dto}", dto),
                options =>
                {
                    options.Destructuring.NotLogged<ThirdPartyDto>(nameof(ThirdPartyDto.ApiKey));
                    options.Destructuring.Mask<ThirdPartyDto>(
                        nameof(ThirdPartyDto.Card),
                        showLast: 4
                    );
                }
            );

            var logged = root.GetProperty("Dto");
            Assert.Equal("ada", logged.GetProperty("Name").GetString());
            Assert.False(logged.TryGetProperty("ApiKey", out _));
            Assert.Equal("***3456", logged.GetProperty("Card").GetString());
            Assert.DoesNotContain("k-123456", line, StringComparison.Ordinal);
        }

        [Fact]
        public async Task Legacy_destructurama_attributes_are_honored_by_name()
        {
            var dto = new LegacyDto { Name = "ada", ApiKey = "legacy-secret" };
            var (root, line) = await LogAsync(logger =>
                logger.LogInformation("legacy {@Dto}", dto)
            );

            Assert.Equal("ada", root.GetProperty("Dto").GetProperty("Name").GetString());
            Assert.False(root.GetProperty("Dto").TryGetProperty("ApiKey", out _));
            Assert.DoesNotContain("legacy-secret", line, StringComparison.Ordinal);
        }

        [Fact]
        public async Task The_record_byte_budget_degrades_later_holes_to_a_sentinel()
        {
            var (root, _) = await LogAsync(
                logger =>
                    logger.LogInformation(
                        "two {@First} {@Second}",
                        new { Filler = new string('x', 64) },
                        new { Value = 1 }
                    ),
                options => options.Destructuring.MaxEncodedBytesPerRecord = 16
            );

            // The first hole consumed the record's budget; the second degrades explicitly.
            Assert.Equal(JsonValueKind.Object, root.GetProperty("First").ValueKind);
            Assert.Equal("…", root.GetProperty("Second").GetString());
        }

        private static async Task<(JsonElement Root, string Line)> LogAsync(
            Action<ILogger> log,
            Action<HostLoomLoggerOptions>? configure = null
        )
        {
            var options = new HostLoomLoggerOptions();
            configure?.Invoke(options);
            // CA2000: sink ownership transfers to the provider.
#pragma warning disable CA2000
            var sink = new CollectingSink();
#pragma warning restore CA2000
            await using var provider = new HostLoomLoggerProvider(
                new JsonLogFormatter(),
                sink,
                options
            );
            log(provider.CreateLogger("Destructuring"));
            await provider.DisposeAsync();

            var line = Assert.Single(sink.Lines());
            return (JsonDocument.Parse(line).RootElement.Clone(), line);
        }

        private sealed class Order
        {
            public int Id { get; set; } = 42;

            public string Customer { get; set; } = "ada";

            public bool Express { get; set; } = true;

            public decimal Total { get; set; } = 19.99m;

            public Dictionary<string, int> Lines { get; } = new() { ["count"] = 3 };
        }

        private class AccountBase
        {
            public string Owner { get; set; } = "";

            [NotLogged]
            public string Password { get; set; } = "";
        }

        private sealed class Account : AccountBase
        {
            public Account? Nested { get; set; }
        }

        private sealed class Payment
        {
            [LogMasked]
            public string Token { get; set; } = "";

            [LogMasked(ShowFirst = 2, ShowLast = 2)]
            public string Card { get; set; } = "";
        }

        private sealed class Contested
        {
            [NotLogged]
            [LogMasked]
            public string Secret { get; set; } = "";
        }

        private sealed class Volatile
        {
            private readonly string _fine = "ok";

            public string Fine => _fine;

            public string Broken =>
                throw new InvalidOperationException($"secret-in-exception {_fine}");

            public override string ToString() => "secret-in-tostring";
        }

        private sealed class Node
        {
            public string Name { get; set; } = "";

            public Node? Next { get; set; }
        }

        private sealed class ThirdPartyDto
        {
            public string Name { get; set; } = "";

            public string ApiKey { get; set; } = "";

            public string Card { get; set; } = "";
        }

        private sealed class LegacyDto
        {
            public string Name { get; set; } = "";

            [Destructurama.Attributed.NotLogged]
            public string ApiKey { get; set; } = "";
        }

        private sealed class CollectingSink : ILogSink
        {
            private readonly MemoryStream _stream = new();
            private readonly Lock _gate = new();

            public void Write(ReadOnlySpan<byte> payload, CancellationToken cancellationToken)
            {
                lock (_gate)
                {
                    _stream.Write(payload);
                }
            }

            public ValueTask FlushAsync(CancellationToken cancellationToken) =>
                ValueTask.CompletedTask;

            public ValueTask DisposeAsync() => ValueTask.CompletedTask;

            public string[] Lines()
            {
                lock (_gate)
                {
                    return Encoding
                        .UTF8.GetString(_stream.ToArray())
                        .Split('\n', StringSplitOptions.RemoveEmptyEntries);
                }
            }
        }
    }
}
