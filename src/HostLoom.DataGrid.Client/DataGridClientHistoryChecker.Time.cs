namespace HostLoom.DataGrid.Client;

public static partial class DataGridClientHistoryChecker
{
    private readonly record struct Moment(int Variable, long Offset = 0)
    {
        internal static Moment At(long ticks) => new(0, ticks);
    }

    // Closed difference bounds: x[left] - x[right] <= bound. No physical clock samples or
    // enumerated timestamps enter the reference model. Int128 prevents path-sum overflow.
    private sealed class GridClock
    {
        private const int Capacity = 65;
        private static readonly Int128 Infinity = Int128.MaxValue / 4;
        private Int128[] _bounds;
        private int _count;

        internal GridClock()
        {
            _bounds = [0];
            _count = 1; // Variable zero is the fixed origin.
        }

        private GridClock(GridClock other)
        {
            _bounds = (Int128[])other._bounds.Clone();
            _count = other._count;
        }

        internal GridClock Copy() => new(this);

        internal int Advance(int previous)
        {
            if (_count == Capacity)
                throw new InvalidOperationException("History time-variable capacity.");
            var next = _count;
            var previousBounds = _bounds;
            _count++;
            _bounds = new Int128[_count * _count];
            Array.Fill(_bounds, Infinity);
            for (var i = 0; i < _count; i++)
                _bounds[i * _count + i] = 0;
            for (var i = 0; i < next; i++)
                Array.Copy(previousBounds, i * next, _bounds, i * _count, next);
            if (
                !Before(Moment.At(0), new(next))
                || !Before(new(next), Moment.At(DateTimeOffset.MaxValue.Ticks))
                || !Before(new(previous), new(next))
            )
                throw new InvalidOperationException("Inconsistent history clock.");
            return next;
        }

        internal bool Equal(Moment left, Moment right) =>
            Before(left, right) && Before(right, left);

        internal bool Before(Moment left, Moment right, bool strict = false)
        {
            var bound = (Int128)right.Offset - left.Offset - (strict ? 1 : 0);
            var index = left.Variable * _count + right.Variable;
            if (_bounds[index] <= bound)
                return true;
            if (_bounds[right.Variable * _count + left.Variable] + bound < 0)
                return false;
            Span<Int128> toLeft = stackalloc Int128[Capacity];
            Span<Int128> fromRight = stackalloc Int128[Capacity];
            for (var i = 0; i < _count; i++)
            {
                toLeft[i] = _bounds[i * _count + left.Variable];
                fromRight[i] = _bounds[right.Variable * _count + i];
            }
            for (var i = 0; i < _count; i++)
            for (var j = 0; j < _count; j++)
            {
                if (toLeft[i] == Infinity || fromRight[j] == Infinity)
                    continue;
                var candidate = toLeft[i] + bound + fromRight[j];
                var position = i * _count + j;
                if (candidate < _bounds[position])
                    _bounds[position] = candidate;
            }
            return true;
        }
    }
}
