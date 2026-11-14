using System.Diagnostics;
using System.Security.Cryptography;
using HostLoom.DataGrid;
using HostLoom.DataGrid.Consensus;
using HostLoom.DataGrid.Storage;
using Xunit;

namespace HostLoom.Tests;

[CollectionDefinition(nameof(DataGridDiskFullTests), DisableParallelization = true)]
public sealed class DataGridDiskFullCollection;

[Collection(nameof(DataGridDiskFullTests))]
public sealed class DataGridDiskFullTests(ITestOutputHelper output)
{
    internal static readonly DataGridStorageLimits Limits = new(FreeReserveBytes: 0);

    [Fact(Timeout = 20000)]
    public void ReserveRefusesBeforeIoWithoutFaultingOrChangingSelectedState()
    {
        using var volume = new OwnedDiskFullVolume(0);
        using var peer = new DataGridPeerFixture();
        using (
            var initial = DataGridFileStore.Bootstrap(
                volume.StorePath,
                peer.Configuration,
                0,
                Limits
            )
        )
            Prepare(initial, peer.Configuration);
        var before = Files(volume.StorePath);
        var wrote = false;
        using (
            var store = DataGridFileStore.Open(
                volume.StorePath,
                peer.Configuration,
                0,
                new(FreeReserveBytes: 1024 * 1024),
                _ => wrote = true
            )
        )
        {
            var candidate = Next(store.Image, peer.Configuration);
            volume.Fill();
            var error = Assert.Throws<IOException>(() =>
                store.Persist(candidate, TestContext.Current.CancellationToken)
            );
            Assert.Equal("Insufficient storage publication reservation.", error.Message);
            Assert.False(wrote);
            Assert.False(store.IsFaulted);
            Assert.Equal(before, Files(volume.StorePath));
            volume.Release();
            store.Persist(candidate, TestContext.Current.CancellationToken);
            Assert.Equal(3, store.Image.CommitIndex);
        }
        using var recovered = DataGridFileStore.Open(
            volume.StorePath,
            peer.Configuration,
            0,
            Limits
        );
        Assert.Equal(3, recovered.Image.CommitIndex);
    }

    [Theory(Timeout = 20000)]
    [InlineData(0)]
    [InlineData(4096)]
    public void ActualWalEnospcEitherRecoversCleanPrefixOrRefusesTornTail(int remainingBytes)
    {
        using var volume = new OwnedDiskFullVolume(0);
        using var peer = new DataGridPeerFixture();
        using (
            var initial = DataGridFileStore.Bootstrap(
                volume.StorePath,
                peer.Configuration,
                0,
                Limits
            )
        )
            Prepare(initial, peer.Configuration);
        var selected = Selected(volume.StorePath);
        var prefix = File.ReadAllBytes(selected);
        var anchor = File.ReadAllBytes(Path.Combine(volume.StorePath, "CURRENT"));
        var steps = new List<DataGridStorageStep>();
        using var store = DataGridFileStore.Open(
            volume.StorePath,
            peer.Configuration,
            0,
            Limits,
            step =>
            {
                steps.Add(step);
                if (step == DataGridStorageStep.WalWriteStarting)
                    volume.Fill(remainingBytes);
            }
        );
        var candidate = Next(store.Image, peer.Configuration);
        var progress = store.Progress;
        try
        {
            var error = Assert.Throws<IOException>(() =>
                store.Persist(candidate, TestContext.Current.CancellationToken)
            );
            Enospc(error);
            output.WriteLine(error.ToString());
            Assert.True(store.IsFaulted);
            Assert.Equal(progress.PublishedWalBytes, store.Progress.PublishedWalBytes);
            Assert.DoesNotContain(DataGridStorageStep.WalWritten, steps);
            Assert.Throws<InvalidOperationException>(() =>
                store.Persist(candidate, TestContext.Current.CancellationToken)
            );
        }
        finally
        {
            DisposeFaulted(store); // Keep the filesystem full until buffered handles have closed.
        }
        var failed = File.ReadAllBytes(selected);
        output.WriteLine(
            $"WAL remaining={remainingBytes}: ENOSPC during Write; selected bytes {prefix.Length}->{failed.Length}; no Persist completion."
        );
        Assert.Equal(prefix, failed.AsSpan(0, prefix.Length).ToArray());
        Assert.Equal(anchor, File.ReadAllBytes(Path.Combine(volume.StorePath, "CURRENT")));
        volume.Release();
        if (remainingBytes != 0)
        {
            Assert.InRange(failed.Length - prefix.Length, 1, remainingBytes);
            var beforeRefusal = Files(volume.StorePath);
            Assert.Throws<InvalidDataException>(() =>
                DataGridFileStore.Open(volume.StorePath, peer.Configuration, 0, Limits)
            );
            Assert.Equal(beforeRefusal, Files(volume.StorePath));
            output.WriteLine(
                "Partial WAL remains preserved and startup refuses without truncation, repair or fallback; freeing space alone cannot restart this member."
            );
            return;
        }
        Assert.Equal(prefix, failed);
        using var recovered = DataGridFileStore.Open(
            volume.StorePath,
            peer.Configuration,
            0,
            Limits
        );
        Assert.Equal(2, recovered.Image.CommitIndex);
        AssertOriginalReplay(recovered.RestoreApplication(), 3);
        recovered.Persist(candidate, TestContext.Current.CancellationToken);
        var state = recovered.RestoreApplication();
        var version = state.Read(DataGridPeerFixture.Map, DataGridPeerFixture.Key)!.Version;
        Assert.Equal(3, version.LogIndex);
        Assert.Equal(version, state.Apply(4, Mutation(2, new byte[65536])).GetItem(0).Version);
    }

    [Theory(Timeout = 20000)]
    [InlineData((int)DataGridStorageStep.SnapshotWriteStarting)]
    [InlineData((int)DataGridStorageStep.GenerationWriteStarting)]
    [InlineData((int)DataGridStorageStep.AnchorWriteStarting)]
    public void ActualPublicationEnospcPreservesCurrentAndAllowsRestartAndIdentityReplay(int phase)
    {
        using var volume = new OwnedDiskFullVolume(0);
        using var peer = new DataGridPeerFixture();
        using (
            var initial = DataGridFileStore.Bootstrap(
                volume.StorePath,
                peer.Configuration,
                0,
                Limits
            )
        )
            Prepare(initial, peer.Configuration);
        var before = Files(volume.StorePath);
        var selected = Selected(volume.StorePath);
        var original = File.ReadAllBytes(selected);
        var anchor = File.ReadAllBytes(Path.Combine(volume.StorePath, "CURRENT"));
        var steps = new List<DataGridStorageStep>();
        using var store = DataGridFileStore.Open(
            volume.StorePath,
            peer.Configuration,
            0,
            Limits,
            step =>
            {
                steps.Add(step);
                if ((int)step == phase)
                    volume.Fill();
            }
        );
        var snapshot = store.RestoreApplication().SaveSnapshot(Guid.NewGuid(), 1);
        var candidate = new RaftDurableImage(peer.Configuration, 1, 0, [], 2, snapshot);
        try
        {
            var error = Assert.Throws<IOException>(() =>
                store.Persist(candidate, TestContext.Current.CancellationToken)
            );
            Enospc(error);
            output.WriteLine(error.ToString());
            Assert.True(store.IsFaulted);
            Assert.Equal((DataGridStorageStep)phase, steps[^1]);
            Assert.Throws<InvalidOperationException>(() =>
                store.RollGeneration(TestContext.Current.CancellationToken)
            );
        }
        finally
        {
            DisposeFaulted(store);
        }
        Assert.Equal(anchor, File.ReadAllBytes(Path.Combine(volume.StorePath, "CURRENT")));
        Assert.Equal(original, File.ReadAllBytes(selected));
        Assert.All(before, entry => Assert.Contains(entry, Files(volume.StorePath)));
        output.WriteLine(
            $"{(DataGridStorageStep)phase}: real ENOSPC; old CURRENT/WAL unchanged; {Directory.GetFiles(volume.StorePath).Length} files retained. Snapshot uses Write; seed/anchor are buffered and fail on flush."
        );
        volume.Release();
        using var recovered = DataGridFileStore.Open(
            volume.StorePath,
            peer.Configuration,
            0,
            Limits
        );
        Assert.Equal(2, recovered.Image.CommitIndex);
        Assert.Equal(0, recovered.Image.SnapshotIndex);
        AssertOriginalReplay(recovered.RestoreApplication(), 3);
        recovered.Persist(candidate, TestContext.Current.CancellationToken);
        Assert.Equal(2, recovered.Image.SnapshotIndex);
        AssertOriginalReplay(recovered.RestoreApplication(), 3);
        recovered.Dispose();
        using var reopened = DataGridFileStore.Open(
            volume.StorePath,
            peer.Configuration,
            0,
            Limits
        );
        Assert.Equal(2, reopened.Image.SnapshotIndex);
        AssertOriginalReplay(reopened.RestoreApplication(), 3);
    }

    [Theory(Timeout = 20000)]
    [InlineData(false)]
    [InlineData(true)]
    public void CorruptOrTornSelectedWalOnBoundedFilesystemIsPreservedAndNeverFallsBack(bool torn)
    {
        using var volume = new OwnedDiskFullVolume(0);
        using var peer = new DataGridPeerFixture();
        using (
            var store = DataGridFileStore.Bootstrap(volume.StorePath, peer.Configuration, 0, Limits)
        )
        {
            Prepare(store, peer.Configuration);
            store.RollGeneration(TestContext.Current.CancellationToken); // A valid older generation must not become a recovery fallback.
        }
        TestContext.Current.CancellationToken.ThrowIfCancellationRequested();
        var path = Selected(volume.StorePath);
        using (var file = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None))
        {
            if (torn)
                file.SetLength(file.Length - 1);
            else
            {
                file.Position = file.Length - 1;
                var value = file.ReadByte();
                file.Position--;
                file.WriteByte((byte)(value ^ 1));
            }
            file.Flush(true);
        }
        var before = Files(volume.StorePath);
        Assert.Throws<InvalidDataException>(() =>
            DataGridFileStore.Open(volume.StorePath, peer.Configuration, 0, Limits)
        );
        Assert.Equal(before, Files(volume.StorePath));
        output.WriteLine(
            $"Selected {(torn ? "torn" : "corrupt")} WAL: startup refused; every file hash unchanged; older valid generation ignored."
        );
    }

    private static void Prepare(DataGridFileStore store, DataGridBootstrapConfiguration config)
    {
        var open = Entry(
            1,
            DataGridCommittedCommand.OpenSession(
                DataGridPeerFixture.Principal,
                Guid.NewGuid(),
                new(5 * TimeSpan.TicksPerSecond),
                default
            )
        );
        store.Persist(new(config, 1, 0, [open], 1), TestContext.Current.CancellationToken);
        // Align a valid durable prefix to the tmpfs page boundary so the zero-space case
        // cannot consume unused bytes in the selected WAL's final allocated page.
        var fixedBytes =
            52
            + DataGridWalRecovery.Append(Entry(2, Mutation(1, [1]))).Length
            + 52
            + DataGridWalRecovery.Position(2, 1).Length
            - 1;
        var length =
            8192 + (int)((4096 - (store.Progress.PublishedWalBytes + fixedBytes) % 4096) % 4096);
        store.Persist(
            new(config, 1, 0, [open, Entry(2, Mutation(1, new byte[length]))], 2),
            TestContext.Current.CancellationToken
        );
        Assert.Equal(0, store.Progress.PublishedWalBytes % 4096);
    }

    private static RaftDurableImage Next(
        RaftDurableImage image,
        DataGridBootstrapConfiguration config
    ) => new(config, 1, 0, image.Entries.Append(Entry(3, Mutation(2, new byte[65536]))), 3);

    private static RaftEntry Entry(long index, DataGridCommittedCommand command) =>
        new(index, 1, DataGridCommittedCommandCodec.Encode(command));

    private static DataGridCommittedCommand Mutation(ulong sequence, byte[] value) =>
        DataGridCommittedCommand.Mutate(
            DataGridPeerFixture.Principal,
            new(
                DataGridMutationKind.Put,
                DataGridPeerFixture.Map,
                1,
                sequence,
                DataGridExpiryKind.Never,
                0,
                [new(DataGridPeerFixture.Key, value)]
            ),
            default
        );

    private static void AssertOriginalReplay(DataGridLocalStateMachine state, long index)
    {
        var original = state.Read(DataGridPeerFixture.Map, DataGridPeerFixture.Key)!;
        var replay = state.Apply(index, Mutation(1, original.GetValue()));
        Assert.Equal(DataGridApplyStatus.Completed, replay.Status);
        Assert.Equal(original.Version, replay.GetItem(0).Version);
        Assert.Equal(2, original.Version.LogIndex);
    }

    internal static string Selected(string directory) =>
        Path.Combine(
            directory,
            "generation-"
                + DataGridCurrentAnchor
                    .ReadFrom(File.ReadAllBytes(Path.Combine(directory, "CURRENT")))
                    .GenerationId.ToString("N")
                + ".wal"
        );

    internal static string[] Files(string directory) =>
        Directory
            .GetFiles(directory)
            .Order(StringComparer.Ordinal)
            .Select(path =>
                Path.GetFileName(path)
                + ":"
                + (
                    Path.GetFileName(path) == "LOCK"
                        ? new FileInfo(path).Length.ToString(
                            System.Globalization.CultureInfo.InvariantCulture
                        )
                        : Convert.ToHexString(SHA256.HashData(File.ReadAllBytes(path)))
                )
            )
            .ToArray();

    internal static void Enospc(IOException error) => Assert.Equal(28, error.HResult & 0xffff);

    private void DisposeFaulted(DataGridFileStore store)
    {
        try
        {
            store.Dispose();
        }
        catch (IOException error)
        {
            Enospc(error);
            output.WriteLine(
                "Dispose also reported ENOSPC while the fixture remained full; ownership was released by finally."
            );
        }
        Assert.True(store.Progress.Closed);
    }
}

/// <summary>Opt-in fault fixture; never fills the host filesystem or an arbitrary supplied path.</summary>
internal sealed class OwnedDiskFullVolume : IDisposable
{
    private readonly string _run;
    private readonly string _filler;
    private readonly string _mount;
    internal string StorePath { get; }

    internal OwnedDiskFullVolume(int member)
    {
        Assert.SkipUnless(
            Environment.GetEnvironmentVariable("HOSTLOOM_DATA_GRID_DISK_FULL") == "1",
            "Requires explicit owned bounded tmpfs disk-full fixture."
        );
        Assert.True(OperatingSystem.IsLinux() && File.Exists("/.dockerenv"));
        Assert.Equal(4096, Environment.SystemPageSize);
        Assert.InRange(member, 0, 2);
        _mount = "/hostloom-grid-enospc-" + member;
        var owner = Environment.GetEnvironmentVariable("HOSTLOOM_DATA_GRID_DISK_FULL_OWNER");
        Assert.True(Guid.TryParseExact(owner, "N", out _));
        Assert.Equal(owner, File.ReadAllText(Path.Combine(_mount, "OWNER")));
        Assert.Equal(
            FileAttributes.Directory,
            File.GetAttributes(_mount) & (FileAttributes.Directory | FileAttributes.ReparsePoint)
        );
        var mount = Assert.Single(
            File.ReadAllLines("/proc/self/mountinfo"),
            line => line.Split(' ')[4] == _mount
        );
        Assert.Equal("tmpfs", mount.Split(" - ", StringSplitOptions.None)[1].Split(' ')[0]);
        var drive = new DriveInfo(_mount);
        Assert.Equal(32L * 1024 * 1024, drive.TotalSize);
        Assert.True(drive.AvailableFreeSpace > 30L * 1024 * 1024);
        Assert.Empty(Directory.GetDirectories(_mount));
        Assert.Equal(["OWNER"], Directory.GetFiles(_mount).Select(Path.GetFileName).ToArray());
        _run = Path.Combine(_mount, "run-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_run);
        StorePath = Path.Combine(_run, "member");
        Directory.CreateDirectory(StorePath);
        _filler = Path.Combine(_run, "filler");
    }

    internal void Fill(int remainingBytes = 0)
    {
        Assert.InRange(remainingBytes, 0, 4096);
        Assert.False(File.Exists(_filler));
        var watch = Stopwatch.StartNew();
        using var file = new FileStream(
            _filler,
            FileMode.CreateNew,
            FileAccess.Write,
            FileShare.None,
            1
        );
        var block = new byte[65536];
        var failed = false;
        try
        {
            for (var written = 0; written <= 32 * 1024 * 1024; written += block.Length)
            {
                Assert.True(
                    watch.Elapsed < TimeSpan.FromSeconds(5),
                    "Abort: bounded fill exceeded five seconds."
                );
                file.Write(block);
            }
        }
        catch (IOException error)
        {
            DataGridDiskFullTests.Enospc(error);
            failed = true;
        }
        Assert.True(failed, "Abort: 32-MiB filesystem did not report ENOSPC within its capacity.");
        Assert.Equal(0, new DriveInfo(_mount).AvailableFreeSpace);
        if (remainingBytes != 0)
            file.SetLength(file.Length - remainingBytes);
        Assert.Equal(remainingBytes, new DriveInfo(_mount).AvailableFreeSpace);
    }

    internal void Release() => File.Delete(_filler);

    public void Dispose()
    {
        // Only this fixture's random directory, including its filler and store, is disposable.
        Directory.Delete(_run, recursive: true);
    }
}
