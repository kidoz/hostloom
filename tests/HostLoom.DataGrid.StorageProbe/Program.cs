using System.Globalization;
using HostLoom.DataGrid;
using HostLoom.DataGrid.Consensus;
using HostLoom.DataGrid.Storage;

var cluster = new Guid("10112233-4455-6677-8899-aabbccddeeff");
var map = new Guid("00112233-4455-6677-8899-aabbccddeeff");
var config = new DataGridBootstrapConfiguration(
    cluster,
    [
        new(new Guid(1, 0, 0, new byte[8]), "node-a.example", 7400),
        new(new Guid(2, 0, 0, new byte[8]), "node-b.example", 7400),
        new(new Guid(3, 0, 0, new byte[8]), "node-c.example", 7400),
    ],
    [
        new(
            map,
            "catalog",
            DataGridMapOwnership.Derived,
            "catalog-service",
            "catalog-loader",
            1000,
            1048576,
            new(1, 1, 2, 1, 1)
        ),
    ]
);
var stage = int.Parse(args[2], CultureInfo.InvariantCulture);
using var store = DataGridFileStore.Open(
    args[0],
    config,
    0,
    observe: step =>
    {
        if ((int)step == stage)
            Pause();
    }
);
if (args[1] == "hold")
    Pause();
else if (args[1] == "snapshot")
{
    var snapshot = store
        .RestoreApplication()
        .SaveSnapshot(new Guid("40112233-4455-6677-8899-aabbccddeeff"), 1);
    store.Persist(new(config, 3, 2, store.Image.Entries.Skip(2), 2, snapshot));
}
else if (args[1] == "append")
{
    var entry = new RaftEntry(
        5,
        4,
        DataGridCommittedCommandCodec.Encode(DataGridCommittedCommand.AdvanceTime(new(100)))
    );
    store.Persist(new(config, 4, 0, store.Image.Entries.Append(entry), 5));
}
else
    throw new InvalidOperationException("Unknown storage probe operation.");

static void Pause()
{
    Console.WriteLine("READY");
    Console.Out.Flush();
    Console.ReadLine(); // The owning test kills this process without disposal after observing READY.
    throw new InvalidOperationException("Probe unexpectedly resumed.");
}
