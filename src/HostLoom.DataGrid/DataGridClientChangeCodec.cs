namespace HostLoom.DataGrid;

internal static class DataGridClientChangeCodec
{
    internal static void WriteCursor(GridWriter writer, DataGridChangeCursor cursor)
    {
        writer.Id(cursor.Generation);
        writer.Id(cursor.Map);
        writer.Number((ulong)cursor.Sequence, 8);
    }

    internal static DataGridChangeCursor ReadCursor(ref GridReader reader) =>
        new(reader.Id(), reader.Id(), reader.Signed());

    internal static void WritePage(GridWriter writer, DataGridChangePage page)
    {
        WriteCursor(writer, page.Cursor);
        writer.Number((ulong)page.AppliedTime.UtcTicks, 8);
        writer.Number((ulong)page.AppliedIndex, 8);
        writer.Number(page.Gap ? 1u : 0u, 1);
        writer.Number(page.HasMore ? 1u : 0u, 1);
        writer.Number((ulong)page.Changes.Count, 2);
        foreach (var change in page.Changes)
        {
            writer.Number((ulong)change.Index, 8);
            writer.Key(change.Key);
        }
    }

    internal static DataGridChangePage ReadPage(
        ref GridReader reader,
        DataGridClientRequest request
    )
    {
        var cursor = ReadCursor(ref reader);
        var time = reader.Time();
        var applied = reader.Signed();
        var gap = reader.Flag();
        var more = reader.Flag();
        var count = reader.Count(2, DataGridChangePage.MaxChanges);
        if (
            cursor.Map != request.MapId
            || cursor.Sequence < count
            || applied <= 0
            || more && count != DataGridChangePage.MaxChanges
        )
            throw new InvalidDataException("Invalid change page header.");
        if (request.ChangeCursor is not { } after)
        {
            if (gap || count != 0)
                throw new InvalidDataException("Initial change position must be at the tail.");
        }
        else if (
            gap
                ? count != 0
                : cursor.Map != after.Map
                    || cursor.Generation != after.Generation
                    || cursor.Sequence - after.Sequence < count
        )
            throw new InvalidDataException("Change page does not follow the requested position.");

        var changes = new DataGridCommittedChange[count];
        long previous = 1;
        for (var i = 0; i < changes.Length; i++)
        {
            var index = reader.Signed();
            if (index < previous || index > applied)
                throw new InvalidDataException("Invalid committed change order.");
            changes[i] = new(index, reader.Key(allowEmpty: false)!);
            previous = index;
        }
        return new(cursor, changes, time, applied, gap, more);
    }
}
