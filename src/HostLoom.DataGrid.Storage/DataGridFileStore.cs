using System.ComponentModel;
using System.Diagnostics;
using System.Runtime.InteropServices;
using HostLoom.DataGrid.Consensus;
using Microsoft.Win32.SafeHandles;

namespace HostLoom.DataGrid.Storage;

internal enum DataGridStorageStep
{
    WalWritten,
    WalFlushed,
    SnapshotFlushed,
    SnapshotRenamed,
    SnapshotDirectoryFlushed,
    GenerationFlushed,
    GenerationRenamed,
    GenerationDirectoryFlushed,
    AnchorFlushed,
    AnchorRenamed,
    AnchorDirectoryFlushed,
}

internal sealed record DataGridStorageLimits(
    long WalBytes = 1024L * 1024 * 1024,
    long DirectoryBytes = 4L * 1024 * 1024 * 1024,
    long FreeReserveBytes = 1024L * 1024 * 1024
);

/// <summary>Serialized experimental file store. A completed Persist is the Raft persistence effect boundary.</summary>
/// <remarks>Unix development implementation. Local process tests do not establish power-loss durability.</remarks>
internal sealed partial class DataGridFileStore : IDisposable
{
    private readonly string _directory;
    private readonly DataGridBootstrapConfiguration _configuration;
    private readonly Guid _member;
    private readonly Action<DataGridStorageStep>? _observe;
    private readonly DataGridStorageLimits _limits;
    private readonly FileStream _ownership;
    private readonly SafeFileHandle _directoryHandle;
    private FileStream? _wal;
    private RaftDurableImage _image;
    private long _lastNumber;
    private long _publishedWalBytes;
    private bool _disposed;

    private DataGridFileStore(
        string directory,
        DataGridBootstrapConfiguration configuration,
        int member,
        DataGridStorageLimits? limits,
        Action<DataGridStorageStep>? observe
    )
    {
        if (
            !(OperatingSystem.IsLinux() || OperatingSystem.IsMacOS())
            || RuntimeInformation.ProcessArchitecture
                is not (Architecture.X64 or Architecture.Arm64)
        )
            throw new PlatformNotSupportedException(
                "File-store experiments require Linux/macOS x64 or arm64."
            );
        ArgumentNullException.ThrowIfNull(configuration);
        if (member is < 0 or > 2)
            throw new ArgumentOutOfRangeException(nameof(member));
        _limits = limits ?? new();
        if (
            _limits.WalBytes is < 1 or > 1024L * 1024 * 1024
            || _limits.DirectoryBytes is < 1 or > 4L * 1024 * 1024 * 1024
            || _limits.FreeReserveBytes < 0
        )
            throw new ArgumentOutOfRangeException(nameof(limits));
        _directory = Path.TrimEndingDirectorySeparator(Path.GetFullPath(directory));
        CheckPath(_directory, directory: true);
        _configuration = configuration;
        _member = configuration.GetMember(member).MemberId;
        _observe = observe;
        _image = new(configuration);
        _directoryHandle = OpenDirectory(
            _directory,
            OperatingSystem.IsMacOS() ? 0x00100000 | 0x01000000 | 0x00000100
                : RuntimeInformation.ProcessArchitecture == Architecture.Arm64
                    ? (1 << 14) | (1 << 19) | (1 << 15)
                : (1 << 16) | (1 << 19) | (1 << 17)
        );
        if (_directoryHandle.IsInvalid)
        {
            var error = Marshal.GetLastPInvokeError();
            _directoryHandle.Dispose();
            throw new IOException("Cannot own storage directory.", new Win32Exception(error));
        }
        try
        {
            var lockPath = FilePath("LOCK");
            if (File.Exists(lockPath) || new FileInfo(lockPath).LinkTarget is not null)
                CheckPath(lockPath);
            _ownership = new(lockPath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
            if (_ownership.Length != 0)
                throw new InvalidDataException("Invalid ownership file.");
        }
        catch
        {
            _ownership?.Dispose();
            _directoryHandle.Dispose();
            throw;
        }
    }

    internal bool IsFaulted { get; private set; }
    internal Guid GenerationId { get; private set; }
    internal DataGridStorageProgress Progress =>
        new(
            _publishedWalBytes,
            _image.Snapshot?.Descriptor.ByteCount ?? 0,
            _image.CommitIndex,
            _lastNumber,
            IsFaulted,
            _disposed
        );
    internal RaftDurableImage Image
    {
        get
        {
            EnsureUsable();
            return _image;
        }
    }

    internal void ValidateOwner(DataGridBootstrapConfiguration configuration, int member)
    {
        EnsureUsable();
        if (
            member is < 0 or > 2
            || configuration.IncarnationId != _configuration.IncarnationId
            || configuration.GetMember(member).MemberId != _member
            || !configuration.ComputeDigest().AsSpan().SequenceEqual(_configuration.ComputeDigest())
        )
            throw new InvalidDataException("Storage does not belong to this configured actor.");
    }

    internal static DataGridFileStore Bootstrap(
        string directory,
        DataGridBootstrapConfiguration configuration,
        int member,
        DataGridStorageLimits? limits = null,
        Action<DataGridStorageStep>? observe = null
    )
    {
        var store = new DataGridFileStore(directory, configuration, member, limits, observe);
        try
        {
            if (
                Directory
                    .EnumerateFileSystemEntries(store._directory)
                    .Any(path => Path.GetFileName(path) != "LOCK")
            )
                throw new InvalidDataException(
                    "Bootstrap requires an explicitly empty owned directory."
                );
            store.Publish(store._image, default);
            return store;
        }
        catch
        {
            store.Dispose();
            throw;
        }
    }

    internal static DataGridFileStore Open(
        string directory,
        DataGridBootstrapConfiguration configuration,
        int member,
        DataGridStorageLimits? limits = null,
        Action<DataGridStorageStep>? observe = null
    )
    {
        var started = Stopwatch.GetTimestamp();
        var success = false;
        try
        {
            var store = OpenCore(directory, configuration, member, limits, observe);
            success = true;
            return store;
        }
        finally
        {
            DataGridStorageDiagnostics.Recovered(
                success,
                Stopwatch.GetElapsedTime(started).TotalSeconds
            );
        }
    }

    private static DataGridFileStore OpenCore(
        string directory,
        DataGridBootstrapConfiguration configuration,
        int member,
        DataGridStorageLimits? limits = null,
        Action<DataGridStorageStep>? observe = null
    )
    {
        var store = new DataGridFileStore(directory, configuration, member, limits, observe);
        try
        {
            store.Inventory(validateIdentities: true);
            var anchor = DataGridCurrentAnchor.ReadFrom(
                store.ReadBounded("CURRENT", DataGridCurrentAnchor.EncodedByteCount)
            );
            if (
                anchor.IncarnationId != configuration.IncarnationId
                || anchor.MemberId != store._member
            )
                throw new InvalidDataException("CURRENT belongs to another cluster/member.");
            store.GenerationId = anchor.GenerationId;
            store._wal = store.OpenWal(GenerationName(anchor.GenerationId));
            (store._image, store._lastNumber) = DataGridWalRecovery.Read(
                store._wal,
                configuration,
                store._member,
                anchor.GenerationId,
                store.ReadSnapshot
            );
            store._publishedWalBytes = store._wal.Length;
            store._wal.Position = store._publishedWalBytes;
            return store;
        }
        catch
        {
            store.Dispose();
            throw;
        }
    }

    /// <summary>Returns only after every dependent record is flushed; errors fault this owner.</summary>
    internal void Persist(RaftDurableImage target, CancellationToken cancellationToken = default)
    {
        EnsureUsable();
        ArgumentNullException.ThrowIfNull(target);
        cancellationToken.ThrowIfCancellationRequested();
        ValidateTransition(target);
        if (!SameSnapshot(_image.Snapshot, target.Snapshot))
        {
            Publish(target, cancellationToken);
            return;
        }
        var records = Delta(target);
        if (records.Count == 0)
            return;
        if (_lastNumber + records.Count > DataGridWalRecovery.MaxRecordCount)
            throw new IOException("WAL record capacity requires a new generation.");
        var bytes = records.Sum(record => (long)record.Length);
        Reserve(bytes, 0);
        var walBytes = _wal!.Length;
        if (walBytes + bytes > _limits.WalBytes)
            throw new IOException("WAL capacity requires a new generation.");
        try
        {
            foreach (var record in records)
                _wal.Write(record);
            Reached(DataGridStorageStep.WalWritten, cancellationToken);
            _wal.Flush(flushToDisk: true);
            Reached(DataGridStorageStep.WalFlushed, cancellationToken);
            _lastNumber += records.Count;
            _image = target;
            _publishedWalBytes = walBytes + bytes;
        }
        catch
        {
            IsFaulted = true;
            throw;
        }
    }

    internal void RollGeneration(CancellationToken cancellationToken = default)
    {
        EnsureUsable();
        cancellationToken.ThrowIfCancellationRequested();
        Publish(_image, cancellationToken);
    }

    private void Publish(RaftDurableImage target, CancellationToken cancellationToken)
    {
        var generation = Guid.NewGuid();
        var records = Seed(target, generation);
        var seedBytes = records.Sum(record => (long)record.Length);
        if (seedBytes > _limits.WalBytes)
            throw new IOException("Generation seed exceeds WAL capacity.");
        var snapshot = target.Snapshot;
        var needsImage =
            snapshot is not null
            && !File.Exists(FilePath(SnapshotName(snapshot.Descriptor.SnapshotId)));
        if (snapshot is not null && !needsImage)
        {
            var existing = ReadSnapshot(snapshot.Descriptor);
            if (!existing.GetContent().AsSpan().SequenceEqual(snapshot.GetContent()))
                throw new InvalidDataException("Snapshot identity cannot be overwritten.");
        }
        Reserve(
            checked(
                seedBytes
                + DataGridCurrentAnchor.EncodedByteCount
                + (needsImage ? snapshot!.Descriptor.ByteCount : 0)
            ),
            needsImage ? 3 : 2
        );
        FileStream? nextWal = null;
        try
        {
            if (needsImage)
            {
                var imageTemp = TemporaryName();
                WriteFlushed(imageTemp, snapshot!.GetContent());
                DataGridLocalStateMachine.Restore(
                    _configuration,
                    snapshot.Descriptor,
                    ReadBounded(imageTemp, checked((int)snapshot.Descriptor.ByteCount))
                );
                Reached(DataGridStorageStep.SnapshotFlushed, cancellationToken);
                File.Move(
                    FilePath(imageTemp),
                    FilePath(SnapshotName(snapshot.Descriptor.SnapshotId))
                );
                Reached(DataGridStorageStep.SnapshotRenamed, cancellationToken);
                FlushDirectory();
                Reached(DataGridStorageStep.SnapshotDirectoryFlushed, cancellationToken);
            }
            var walTemp = TemporaryName();
            using (
                var seed = new FileStream(
                    FilePath(walTemp),
                    FileMode.CreateNew,
                    FileAccess.ReadWrite,
                    FileShare.None
                )
            )
            {
                foreach (var record in records)
                    seed.Write(record);
                seed.Position = 0;
                DataGridWalRecovery.Read(seed, _configuration, _member, generation, ReadSnapshot);
                seed.Flush(flushToDisk: true);
            }
            Reached(DataGridStorageStep.GenerationFlushed, cancellationToken);
            File.Move(FilePath(walTemp), FilePath(GenerationName(generation)));
            Reached(DataGridStorageStep.GenerationRenamed, cancellationToken);
            FlushDirectory();
            Reached(DataGridStorageStep.GenerationDirectoryFlushed, cancellationToken);
            nextWal = OpenWal(GenerationName(generation));
            nextWal.Position = nextWal.Length;
            var anchorTemp = TemporaryName();
            WriteFlushed(
                anchorTemp,
                new DataGridCurrentAnchor(
                    _configuration.IncarnationId,
                    _member,
                    generation
                ).Encode()
            );
            Reached(DataGridStorageStep.AnchorFlushed, cancellationToken);
            File.Move(FilePath(anchorTemp), FilePath("CURRENT"), overwrite: true);
            Reached(DataGridStorageStep.AnchorRenamed, cancellationToken);
            FlushDirectory();
            Reached(DataGridStorageStep.AnchorDirectoryFlushed, cancellationToken);
            _wal?.Dispose();
            _wal = nextWal;
            nextWal = null;
            _image = target;
            GenerationId = generation;
            _lastNumber = records.Count;
            _publishedWalBytes = seedBytes;
        }
        catch
        {
            IsFaulted = true;
            throw;
        }
        finally
        {
            nextWal?.Dispose();
        }
    }

    private void ValidateTransition(RaftDurableImage target)
    {
        // Revalidate even a candidate constructed against another configuration.
        _ = new RaftDurableImage(
            _configuration,
            target.Term,
            target.Vote,
            target.Entries,
            target.CommitIndex,
            target.Snapshot
        );
        if (
            target.Term < _image.Term
            || target.CommitIndex < _image.CommitIndex
            || (target.Term == _image.Term && _image.Vote is not null && target.Vote != _image.Vote)
            || target.SnapshotIndex < _image.SnapshotIndex
        )
            throw new InvalidDataException("Persistent state cannot move backwards.");
        if (SameSnapshot(_image.Snapshot, target.Snapshot))
        {
            foreach (var entry in _image.Entries.Where(entry => entry.Index <= _image.CommitIndex))
                if (target.LastIndex < entry.Index || !entry.SameAs(target.EntryAt(entry.Index)))
                    throw new InvalidDataException("Committed history cannot change.");
            return;
        }
        if (target.Snapshot is null || target.SnapshotIndex < _image.CommitIndex)
            throw new InvalidDataException("Snapshot must cover the durable committed prefix.");
        var matches =
            target.SnapshotIndex <= _image.LastIndex
            && _image.TermAt(target.SnapshotIndex) == target.Snapshot.Descriptor.LastIncludedTerm;
        var expectedSuffix = matches
            ? _image.Entries.Where(entry => entry.Index > target.SnapshotIndex).ToArray()
            : [];
        if (
            expectedSuffix.Length != target.Entries.Count
            || expectedSuffix.Where((entry, i) => !entry.SameAs(target.Entries[i])).Any()
        )
            throw new InvalidDataException("Snapshot transition changed its required suffix.");
        if (matches)
        {
            var application = RestoreApplication(_image);
            while (application.LastAppliedIndex < target.SnapshotIndex)
            {
                var entry = _image.EntryAt(application.LastAppliedIndex + 1);
                application.Apply(
                    entry.Index,
                    DataGridCommittedCommandCodec.ReadFrom(entry.GetCommand())
                );
            }
            var expected = application.SaveSnapshot(
                target.Snapshot.Descriptor.SnapshotId,
                target.Snapshot.Descriptor.LastIncludedTerm
            );
            if (!expected.GetContent().AsSpan().SequenceEqual(target.Snapshot.GetContent()))
                throw new InvalidDataException("Snapshot disagrees with the known log prefix.");
        }
    }

    private List<byte[]> Delta(RaftDurableImage target)
    {
        var records = new List<byte[]>();
        void Add(DataGridWalRecordKind kind, byte[] payload) =>
            records.Add(Record(kind, checked(_lastNumber + records.Count + 1), payload));
        if (target.Term != _image.Term || target.Vote != _image.Vote)
            Add(
                DataGridWalRecordKind.HardState,
                DataGridWalRecovery.HardState(_configuration, target)
            );
        var common = _image.SnapshotIndex;
        while (
            common < Math.Min(_image.LastIndex, target.LastIndex)
            && _image.EntryAt(common + 1).SameAs(target.EntryAt(common + 1))
        )
            common++;
        if (common < _image.LastIndex)
            Add(DataGridWalRecordKind.LogTruncate, DataGridWalRecovery.Position(common + 1));
        foreach (var entry in target.Entries.Where(entry => entry.Index > common))
            Add(DataGridWalRecordKind.LogAppend, DataGridWalRecovery.Append(entry));
        if (target.CommitIndex != _image.CommitIndex)
            Add(
                DataGridWalRecordKind.CommitCheckpoint,
                DataGridWalRecovery.Position(target.CommitIndex, target.TermAt(target.CommitIndex))
            );
        return records;
    }

    private List<byte[]> Seed(RaftDurableImage image, Guid generation)
    {
        var records = new List<byte[]>();
        void Add(DataGridWalRecordKind kind, byte[] payload) =>
            records.Add(Record(kind, records.Count + 1, payload));
        Add(
            DataGridWalRecordKind.Identity,
            DataGridWalRecovery.Identity(_configuration, _member, generation)
        );
        Add(DataGridWalRecordKind.HardState, DataGridWalRecovery.HardState(_configuration, image));
        if (image.Snapshot is { } snapshot)
            Add(DataGridWalRecordKind.SnapshotManifest, snapshot.Descriptor.Encode());
        foreach (var entry in image.Entries)
            Add(DataGridWalRecordKind.LogAppend, DataGridWalRecovery.Append(entry));
        Add(
            DataGridWalRecordKind.CommitCheckpoint,
            DataGridWalRecovery.Position(image.CommitIndex, image.TermAt(image.CommitIndex))
        );
        return records;
    }

    internal DataGridLocalStateMachine RestoreApplication()
    {
        EnsureUsable();
        return RestoreApplication(_image);
    }

    private DataGridLocalStateMachine RestoreApplication(RaftDurableImage image)
    {
        var application = image.Snapshot is { } snapshot
            ? DataGridLocalStateMachine.Restore(
                _configuration,
                snapshot.Descriptor,
                snapshot.GetContent()
            )
            : new(_configuration);
        while (application.LastAppliedIndex < image.CommitIndex)
        {
            var entry = image.EntryAt(application.LastAppliedIndex + 1);
            application.Apply(
                entry.Index,
                DataGridCommittedCommandCodec.ReadFrom(entry.GetCommand())
            );
        }
        return application;
    }

    private DataGridLocalSnapshot ReadSnapshot(DataGridSnapshotDescriptor descriptor)
    {
        if (descriptor.ByteCount > RaftDurableImage.MaxImageBytes)
            throw new InvalidDataException("Snapshot fixture capacity exceeded.");
        var bytes = ReadBounded(
            SnapshotName(descriptor.SnapshotId),
            checked((int)descriptor.ByteCount)
        );
        DataGridLocalStateMachine.Restore(_configuration, descriptor, bytes);
        return new(descriptor, bytes);
    }

    private (long Bytes, int Count) Inventory(bool validateIdentities = false)
    {
        long bytes = 0;
        var count = 0;
        Span<byte> header = stackalloc byte[56];
        Span<byte> incarnation = stackalloc byte[16];
        _configuration.IncarnationId.TryWriteBytes(incarnation, bigEndian: true, out _);
        foreach (var path in Directory.EnumerateFileSystemEntries(_directory))
        {
            if (++count > 4096)
                throw new IOException("Storage file inventory exceeded.");
            CheckPath(path);
            var name = Path.GetFileName(path);
            var size = new FileInfo(path).Length;
            bytes = checked(bytes + size);
            if (name is "CURRENT" or "LOCK")
                continue;
            if (ParseName(name, "pending-", ".tmp", out _))
                continue;
            if (ParseName(name, "generation-", ".wal", out var generation))
            {
                if (size > _limits.WalBytes)
                    throw new IOException("WAL capacity exceeded.");
                if (validateIdentities)
                {
                    using var wal = File.OpenRead(path);
                    var record = DataGridWalRecovery.ReadRecord(wal, 1);
                    if (
                        record is not { Kind: DataGridWalRecordKind.Identity }
                        || !record
                            .GetPayload()
                            .AsSpan()
                            .SequenceEqual(
                                DataGridWalRecovery.Identity(_configuration, _member, generation)
                            )
                    )
                        throw new InvalidDataException("Foreign generation in owned directory.");
                }
                continue;
            }
            if (ParseName(name, "snapshot-", ".bin", out _))
            {
                if (size > RaftDurableImage.MaxImageBytes)
                    throw new IOException("Snapshot capacity exceeded.");
                if (validateIdentities)
                {
                    using var image = File.OpenRead(path);
                    try
                    {
                        image.ReadExactly(header);
                    }
                    catch (EndOfStreamException exception)
                    {
                        throw new InvalidDataException("Invalid snapshot header.", exception);
                    }
                    if (
                        !header[..4].SequenceEqual("HLGS"u8)
                        || !header.Slice(8, 16).SequenceEqual(incarnation)
                        || !header[24..].SequenceEqual(_configuration.ComputeDigest())
                    )
                        throw new InvalidDataException("Foreign snapshot in owned directory.");
                }
                continue;
            }
            throw new InvalidDataException("Foreign file in owned storage directory.");
        }
        if (bytes > _limits.DirectoryBytes)
            throw new IOException("Owned directory capacity exceeded.");
        return (bytes, count);
    }

    private void Reserve(long bytes, int files)
    {
        var inventory = Inventory();
        if (
            bytes > _limits.DirectoryBytes - inventory.Bytes
            || inventory.Count + files > 4096
            || new DriveInfo(_directory).AvailableFreeSpace - bytes < _limits.FreeReserveBytes
        )
            throw new IOException("Insufficient storage publication reservation.");
    }

    private byte[] ReadBounded(string name, int expectedSize)
    {
        var path = FilePath(name);
        CheckPath(path);
        using var stream = File.OpenRead(path);
        if (stream.Length != expectedSize)
            throw new InvalidDataException("Unexpected storage file length.");
        var bytes = new byte[expectedSize];
        stream.ReadExactly(bytes);
        if (stream.ReadByte() != -1)
            throw new InvalidDataException("Storage file changed during read.");
        return bytes;
    }

    private FileStream OpenWal(string name)
    {
        var path = FilePath(name);
        CheckPath(path);
        return new(path, FileMode.Open, FileAccess.ReadWrite, FileShare.Read);
    }

    private void WriteFlushed(string name, byte[] bytes)
    {
        using var stream = new FileStream(
            FilePath(name),
            FileMode.CreateNew,
            FileAccess.Write,
            FileShare.None
        );
        stream.Write(bytes);
        stream.Flush(flushToDisk: true);
    }

    private void Reached(DataGridStorageStep step, CancellationToken cancellationToken)
    {
        _observe?.Invoke(step);
        cancellationToken.ThrowIfCancellationRequested();
    }

    private void FlushDirectory() => RandomAccess.FlushToDisk(_directoryHandle);

    private string FilePath(string name) => Path.Combine(_directory, name);

    private static string GenerationName(Guid id) => $"generation-{id:N}.wal";

    private static string SnapshotName(Guid id) => $"snapshot-{id:N}.bin";

    private static string TemporaryName() => $"pending-{Guid.NewGuid():N}.tmp";

    private static byte[] Record(DataGridWalRecordKind kind, long number, byte[] payload) =>
        new DataGridWalRecord(kind, number, payload).Encode();

    private static bool SameSnapshot(DataGridLocalSnapshot? left, DataGridLocalSnapshot? right) =>
        left is null
            ? right is null
            : right is not null
                && left.Descriptor.Encode().AsSpan().SequenceEqual(right.Descriptor.Encode());

    private static bool ParseName(string name, string prefix, string suffix, out Guid id)
    {
        id = default;
        return name.Length == prefix.Length + 32 + suffix.Length
            && name.StartsWith(prefix, StringComparison.Ordinal)
            && name.EndsWith(suffix, StringComparison.Ordinal)
            && Guid.TryParseExact(name.AsSpan(prefix.Length, 32), "N", out id)
            && id != Guid.Empty
            && name == prefix + id.ToString("N") + suffix;
    }

    private static void CheckPath(string path, bool directory = false)
    {
        var attributes = File.GetAttributes(path);
        if (
            (attributes & FileAttributes.ReparsePoint) != 0
            || ((attributes & FileAttributes.Directory) != 0) != directory
        )
            throw new IOException("Storage paths must be ordinary owned files/directories.");
    }

    private void EnsureUsable()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (IsFaulted)
            throw new InvalidOperationException(
                "Storage is faulted; validated restart is required."
            );
    }

    public void Dispose()
    {
        if (_disposed)
            return;
        _disposed = true;
        try
        {
            _wal?.Dispose();
        }
        finally
        {
            try
            {
                _ownership.Dispose();
            }
            finally
            {
                _directoryHandle.Dispose();
            }
        }
    }

    [DefaultDllImportSearchPaths(DllImportSearchPath.SafeDirectories)]
    [LibraryImport(
        "libc",
        EntryPoint = "open",
        SetLastError = true,
        StringMarshalling = StringMarshalling.Utf8
    )]
    private static partial SafeFileHandle OpenDirectory(string path, int flags);
}
