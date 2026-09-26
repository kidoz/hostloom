using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using System.Text;
using System.Text.Json;
using HostLoom.Logging;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

// A Native AOT publish of this program must produce no trim or AOT warnings. The program checks
// its own output and exits with 1 on a mismatch, so running the native binary is the test: options
// bound from configuration, preserved and masked types destructured as on the JIT, and a type
// nobody preserved reported instead of silently written without its members.
var destructurerFailures = 0L;
using var listener = new MeterListener();
listener.InstrumentPublished = (instrument, meters) =>
{
    if (
        instrument.Meter.Name == "HostLoom.Logging"
        && instrument.Name == "hostloom.logging.failures"
    )
    {
        meters.EnableMeasurementEvents(instrument);
    }
};
listener.SetMeasurementEventCallback<long>(
    (_, count, tags, _) =>
    {
        foreach (var tag in tags)
        {
            if (tag.Key == "component" && tag.Value is "destructurer")
            {
                Interlocked.Add(ref destructurerFailures, count);
            }
        }
    }
);
listener.Start();

CapturingSink? sink = null;
var builder = Host.CreateApplicationBuilder(args);
builder.Configuration.AddInMemoryCollection(
    new Dictionary<string, string?>
    {
        ["HostLoom:Logging:ServiceName"] = "orders",
        ["HostLoom:Logging:AttachMachineName"] = "false",
        ["HostLoom:Logging:QueueFullPolicy"] = "Block",
        ["HostLoom:Logging:Destructuring:TypeTags"] = "true",
    }
);
builder.Logging.ClearProviders();
builder.Logging.AddHostLoomLogging(
    _ => sink = new CapturingSink(),
    builder.Configuration.GetSection("HostLoom:Logging"),
    // Every type written with {@...} under Native AOT, nested ones included.
    logging => logging.Destructuring.Preserve<Order>().Preserve<Shipment>(),
    new ClefLogFormatter()
);

using (var host = builder.Build())
{
    var logger = host.Services.GetRequiredService<ILoggerFactory>().CreateLogger("Orders");
    Log.Placed(
        logger,
        new Order(7, "eu", ["books", "music"], new Shipment("Oslo", 2), "t-1", "4111111111111111")
    );
    Log.Totals(logger, [1, 2, 3], new Dictionary<string, double> { ["books"] = 1.5 }, [0xCA, 0xFE]);
    Log.Invoiced(logger, new Invoice(3, 99.5m));
} // Disposing the host drains the logging pipeline into the sink.

var lines = sink?.Lines() ?? [];
foreach (var line in lines)
{
    Console.WriteLine(line);
}

if (lines.Length != 3)
{
    Console.WriteLine($"logging mismatches: 3 records expected, got {lines.Length}");
    return 1;
}

var problems = new List<string>();
Expect(
    lines.All(line => Field(line, "ServiceName") == "\"orders\""),
    "ServiceName bound from configuration on every record"
);
Expect(
    Field(lines[0], "Order")
        == """{"Id":7,"Region":"eu","Categories":["books","music"],"ShipTo":{"City":"Oslo","Parcels":2,"$type":"Shipment"},"Card":"***1111","$type":"Order"}""",
    "the preserved order destructured, its token left out and its card masked"
);
Expect(Field(lines[1], "Items") == "[1,2,3]", "a list as a sequence");
Expect(Field(lines[1], "Weights") == """{"books":1.5}""", "a dictionary as an object");
Expect(Field(lines[1], "Receipt") == "\"CAFE\"", "a byte array as hex");

// Nothing preserved Invoice. The JIT reads its members anyway; Native AOT has none to read, so
// it writes the type tag alone and counts the type once. Members gone without a count would fail.
var invoice = Field(lines[2], "Invoice");
var failures = Interlocked.Read(ref destructurerFailures);
var invoiceWhole = invoice == """{"Number":3,"Amount":99.5,"$type":"Invoice"}""" && failures == 0;
var invoiceReported = invoice == """{"$type":"Invoice"}""" && failures == 1;
Expect(
    invoiceWhole || invoiceReported,
    $"the unpreserved invoice written whole or reported, got {invoice} with {failures} failures"
);

Console.WriteLine(
    problems.Count == 0
        ? $"logging verified; unpreserved members {(invoiceReported ? "missing and reported" : "read")}"
        : "logging mismatches: " + string.Join("; ", problems)
);
return problems.Count == 0 ? 0 : 1;

void Expect(bool condition, string what)
{
    if (!condition)
    {
        problems.Add(what);
    }
}

static string? Field(string line, string name)
{
    using var record = JsonDocument.Parse(line);
    return record.RootElement.TryGetProperty(name, out var value) ? value.GetRawText() : null;
}

internal sealed record Shipment(string City, int Parcels);

internal sealed record Order(
    int Id,
    string Region,
    IReadOnlyList<string> Categories,
    Shipment ShipTo,
    [property: NotLogged] string Token,
    [property: LogMasked(ShowLast = 4)] string Card
);

internal sealed record Invoice(int Number, decimal Amount);

internal static partial class Log
{
    [LoggerMessage(Level = LogLevel.Information, Message = "Placed {@Order}")]
    public static partial void Placed(ILogger logger, Order order);

    [LoggerMessage(Level = LogLevel.Information, Message = "Totals {Items} {Weights} {Receipt}")]
    public static partial void Totals(
        ILogger logger,
        List<int> items,
        Dictionary<string, double> weights,
        byte[] receipt
    );

    [LoggerMessage(Level = LogLevel.Information, Message = "Invoiced {@Invoice}")]
    public static partial void Invoiced(ILogger logger, Invoice invoice);
}

/// <summary>Keeps every line the provider writes, for the checks above.</summary>
internal sealed class CapturingSink : ILogSink
{
    private readonly ConcurrentQueue<string> _lines = new();

    public void Write(ReadOnlySpan<byte> payload, CancellationToken cancellationToken)
    {
        foreach (
            var line in Encoding
                .UTF8.GetString(payload)
                .Split('\n', StringSplitOptions.RemoveEmptyEntries)
        )
        {
            _lines.Enqueue(line);
        }
    }

    public ValueTask FlushAsync(CancellationToken cancellationToken) => ValueTask.CompletedTask;

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    public string[] Lines() => [.. _lines];
}
