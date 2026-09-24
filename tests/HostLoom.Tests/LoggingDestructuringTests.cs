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
        public async Task Protection_declared_on_a_virtual_member_survives_an_override()
        {
            var session = new DerivedSession
            {
                Owner = "ada",
                Token = "override-token",
                Password = "override-password",
                ApiKey = "override-apikey",
            };
            var (root, line) = await LogAsync(logger =>
                logger.LogInformation("session {@Session}", session)
            );

            var logged = root.GetProperty("Session");
            Assert.Equal("ada", logged.GetProperty("Owner").GetString());
            // The attributes live on the base declarations; the derived type overrides every
            // protected property. Reflection on the override must still find them, for the
            // native attributes and for the legacy one recognized by name alike.
            Assert.Equal("***", logged.GetProperty("Token").GetString());
            Assert.False(logged.TryGetProperty("Password", out _));
            Assert.False(logged.TryGetProperty("ApiKey", out _));
            Assert.DoesNotContain("override-", line, StringComparison.Ordinal);
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

        [Theory]
        [InlineData("", "***", "***", "***")]
        [InlineData("1234", "***", "***", "***")]
        [InlineData("123456", "***", "123***", "***")]
        [InlineData("1234567", "***", "123***", "***")]
        [InlineData("12345678", "***5678", "123***", "12***78")]
        [InlineData("1234567890123456", "***3456", "123***", "12***56")]
        public async Task LogMasked_never_reveals_more_than_half_of_a_value(
            string value,
            string lastFour,
            string firstThree,
            string firstAndLastTwo
        )
        {
            var secrets = new Secrets
            {
                LastFour = value,
                FirstThree = value,
                FirstAndLastTwo = value,
            };
            var (root, _) = await LogAsync(logger =>
                logger.LogInformation("secrets {@Secrets}", secrets)
            );

            // A value shorter than twice the requested reveal is written as the mask text alone:
            // ShowLast = 4 on a four-digit PIN would otherwise print the whole PIN after "***".
            var logged = root.GetProperty("Secrets");
            Assert.Equal(lastFour, logged.GetProperty("LastFour").GetString());
            Assert.Equal(firstThree, logged.GetProperty("FirstThree").GetString());
            Assert.Equal(firstAndLastTwo, logged.GetProperty("FirstAndLastTwo").GetString());
            if (value.Length is > 0 and < 8)
            {
                // The safe-rendered message is built from the same masked representation.
                Assert.DoesNotContain(
                    value,
                    root.GetProperty("message").GetString(),
                    StringComparison.Ordinal
                );
                Assert.DoesNotContain(value, logged.GetRawText(), StringComparison.Ordinal);
            }
        }

        [Fact]
        public async Task The_per_type_mask_hides_a_value_too_short_to_reveal()
        {
            var dto = new ThirdPartyDto { Name = "ada", Card = "4071" };
            var (root, _) = await LogAsync(
                logger => logger.LogInformation("3p {@Dto}", dto),
                options =>
                    options.Destructuring.Mask<ThirdPartyDto>(
                        nameof(ThirdPartyDto.Card),
                        showLast: 4
                    )
            );

            var logged = root.GetProperty("Dto");
            Assert.Equal("***", logged.GetProperty("Card").GetString());
            Assert.DoesNotContain("4071", logged.GetRawText(), StringComparison.Ordinal);
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
            var filler = new string('x', 20);
            var (root, _) = await LogAsync(
                logger =>
                    logger.LogInformation(
                        "two {@First} {@Second}",
                        new
                        {
                            A = filler,
                            B = filler,
                            C = filler,
                            D = filler,
                        },
                        new { Value = 1 }
                    ),
                options => options.Destructuring.MaxEncodedBytesPerRecord = 128
            );

            // The first hole is cut at the member that would have overrun the record's budget,
            // with room kept for the marker; the second no longer fits and degrades explicitly.
            var first = root.GetProperty("First");
            Assert.Equal(JsonValueKind.Object, first.ValueKind);
            Assert.Equal(filler, first.GetProperty("C").GetString());
            Assert.False(first.TryGetProperty("D", out _));
            Assert.Equal("[Truncated]", first.EnumerateObject().Last().Value.GetString());
            Assert.True(Encoding.UTF8.GetByteCount(first.GetRawText()) <= 128);
            Assert.Equal("…", root.GetProperty("Second").GetString());
        }

        [Fact]
        public async Task An_oversized_dictionary_key_is_capped_like_a_string_value()
        {
            var map = new Dictionary<string, int> { [new string('k', 1_000_000)] = 1 };
            var (root, line) = await LogAsync(logger => logger.LogInformation("keys {@Map}", map));

            // The key keeps MaxStringLength characters and the "…" marker, exactly like a value.
            var member = Assert.Single(root.GetProperty("Map").EnumerateObject());
            Assert.Equal(string.Concat(new string('k', 4096), "…"), member.Name);
            Assert.Equal(1, member.Value.GetInt32());
            Assert.True(
                Encoding.UTF8.GetByteCount(line) < 64 * 1024,
                $"a 1 MB key produced a {Encoding.UTF8.GetByteCount(line)}-byte record"
            );
        }

        [Fact]
        public async Task Many_dictionary_keys_stop_inside_the_record_byte_budget()
        {
            var map = Enumerable
                .Range(0, 500)
                .ToDictionary(i => $"key-{i:D4}-{new string('k', 40)}", _ => new string('v', 40));
            var (root, _) = await LogAsync(
                logger => logger.LogInformation("keys {@Map}", map),
                options =>
                {
                    options.Destructuring.MaxObjectMembers = 10_000;
                    options.Destructuring.MaxEncodedBytesPerRecord = 2048;
                }
            );

            // The budget, not the member cap, ends the walk: every kept member is intact, the
            // cut is marked, and the fragment never grows past the budget.
            var logged = root.GetProperty("Map");
            Assert.True(
                Encoding.UTF8.GetByteCount(logged.GetRawText()) <= 2048,
                $"the field is {Encoding.UTF8.GetByteCount(logged.GetRawText())} bytes"
            );
            var members = logged.EnumerateObject().ToArray();
            Assert.True(members.Length > 2, $"only {members.Length} members were kept");
            Assert.Equal("…", members[^1].Name);
            Assert.Equal("[Truncated]", members[^1].Value.GetString());
            Assert.All(
                members[..^1],
                member => Assert.Equal(new string('v', 40), member.Value.GetString())
            );
        }

        [Fact]
        public async Task A_nested_dictionary_is_capped_and_bounded_by_the_same_budget()
        {
            var inventory = Enumerable
                .Range(0, 200)
                .ToDictionary(
                    i => string.Concat(new string('s', 5000), i.ToString("D3", null)),
                    i => i
                );
            var value = new Dictionary<string, object?>
            {
                ["region"] = "eu",
                ["inventory"] = inventory,
                ["trailer"] = "done",
            };
            var (root, _) = await LogAsync(
                logger => logger.LogInformation("stock {@Stock}", value),
                options => options.Destructuring.MaxEncodedBytesPerRecord = 16 * 1024
            );

            var logged = root.GetProperty("Stock");
            Assert.True(
                Encoding.UTF8.GetByteCount(logged.GetRawText()) <= 16 * 1024,
                $"the field is {Encoding.UTF8.GetByteCount(logged.GetRawText())} bytes"
            );
            Assert.Equal("eu", logged.GetProperty("region").GetString());
            var nested = logged.GetProperty("inventory").EnumerateObject().ToArray();
            Assert.Equal(string.Concat(new string('s', 4096), "…"), nested[0].Name);
            Assert.Equal("…", nested[^1].Name);
            Assert.Equal("[Truncated]", nested[^1].Value.GetString());
        }

        [Fact]
        public void Every_budget_yields_valid_json_that_stays_within_it()
        {
            var value = new Dictionary<string, object?>
            {
                ["region"] = "eu",
                ["items"] = new object?[]
                {
                    1,
                    "книга",
                    null,
                    new { Name = "notebook", Categories = new[] { "books", "music" } },
                    new Dictionary<string, int> { ["x"] = 1, ["y"] = 2 },
                },
                ["nested"] = new { Inner = new { Deep = new[] { new string('z', 50), "ä" } } },
                ["empty"] = new Dictionary<string, int>(),
                ["long"] = new string('q', 300),
            };
            var destructurer = new Destructurer(new DestructuringOptions(), null);
            var full = destructurer.Destructure(value, int.MaxValue).ToArray();
            var rootMarker = "{\"\\u2026\":\"[Truncated]\"}"u8.ToArray();

            for (var budget = 1; budget <= full.Length + 64; budget++)
            {
                var json = destructurer.Destructure(value, budget).ToArray();
                using var document = JsonDocument.Parse(json);
                if (json.AsSpan().SequenceEqual(full))
                {
                    continue;
                }

                // A cut is always marked, and the fragment stays within the budget unless the
                // budget cannot hold even the root's marker — which the capture layer then
                // degrades to a "…" field.
                Assert.Contains(
                    "\\u2026",
                    Encoding.ASCII.GetString(json),
                    StringComparison.Ordinal
                );
                Assert.True(
                    json.Length <= budget || json.AsSpan().SequenceEqual(rootMarker),
                    $"budget {budget} produced {json.Length} bytes"
                );
            }

            // The walk keeps room to mark a cut, so only a budget that much larger than the value
            // is guaranteed to leave it whole.
            Assert.Equal(full, destructurer.Destructure(value, full.Length + 64).ToArray());
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
            // Disposal drains the writer; bounded so a stalled writer fails the test instead of
            // hanging the run.
            await provider
                .DisposeAsync()
                .AsTask()
                .WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);

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

        private class SessionBase
        {
            public string Owner { get; set; } = "";

            [LogMasked]
            public virtual string Token { get; set; } = "";

            [NotLogged]
            public virtual string Password { get; set; } = "";

            [Destructurama.Attributed.NotLogged]
            public virtual string ApiKey { get; set; } = "";
        }

        private sealed class DerivedSession : SessionBase
        {
            public override string Token
            {
                get => base.Token;
                set => base.Token = value;
            }

            public override string Password
            {
                get => base.Password;
                set => base.Password = value;
            }

            public override string ApiKey
            {
                get => base.ApiKey;
                set => base.ApiKey = value;
            }
        }

        private sealed class Payment
        {
            [LogMasked]
            public string Token { get; set; } = "";

            [LogMasked(ShowFirst = 2, ShowLast = 2)]
            public string Card { get; set; } = "";
        }

        private sealed class Secrets
        {
            [LogMasked(ShowLast = 4)]
            public string LastFour { get; set; } = "";

            [LogMasked(ShowFirst = 3)]
            public string FirstThree { get; set; } = "";

            [LogMasked(ShowFirst = 2, ShowLast = 2)]
            public string FirstAndLastTwo { get; set; } = "";
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
