"""Check the Release actor example against a process kill and cold restart."""

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from tempfile import TemporaryDirectory

PROJECT = "HostLoom.Examples.Actors"
DLL = Path(__file__).resolve().parents[1] / f"examples/{PROJECT}/bin/Release/net10.0/{PROJECT}.dll"


def publish(directory: Path, version: int, items: str) -> None:
    (directory / f"eu-{version}.txt").write_text(items, encoding="utf-8")
    pending = directory / "eu.pending"
    pending.write_text(str(version), encoding="ascii")
    pending.replace(directory / "eu.current")


@asynccontextmanager
async def worker(directory: Path) -> AsyncIterator[asyncio.subprocess.Process]:
    child = await asyncio.create_subprocess_exec(
        "dotnet",
        str(DLL),
        "--recover",
        str(directory),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
    )
    try:
        yield child
    finally:
        if child.returncode is None:
            child.kill()
        await asyncio.wait_for(child.wait(), timeout=10)


async def expect(child: asyncio.subprocess.Process, snapshot: bytes) -> None:
    assert child.stdout is not None
    line = await asyncio.wait_for(child.stdout.readline(), timeout=10)
    if line.rstrip() != snapshot:
        raise RuntimeError(f"Expected {snapshot!r}, received {line!r}")


async def main() -> None:
    if not DLL.is_file():
        raise FileNotFoundError(f"Build examples/{PROJECT} in Release first.")
    with TemporaryDirectory(prefix="hostloom-recovery-") as temporary:
        directory = Path(temporary)
        publish(directory, 1, "books\n")
        async with worker(directory) as first:
            await expect(first, b"eu 1 books")
            publish(directory, 2, "books\nmusic\n")
            assert first.stdin is not None
            first.stdin.write(b"refresh\n")
            await first.stdin.drain()
            await expect(first, b"eu 2 books, music")
            first.stdin.write(b"read\n")
            await first.stdin.drain()
            await expect(first, b"eu 2 books, music")
            (directory / "eu-3.txt").write_text("unpublished\n", encoding="utf-8")
            first.kill()
            if await asyncio.wait_for(first.wait(), timeout=10) == 0:
                raise RuntimeError("First worker exited normally.")
        async with worker(directory) as second:
            await expect(second, b"eu 2 books, music")
            assert second.stdin is not None
            second.stdin.close()
            if await asyncio.wait_for(second.wait(), timeout=10) != 0:
                raise RuntimeError("Replacement worker failed to shut down.")
    print("Cold restart recovered eu version 2 with books and music after the worker was killed.")


if __name__ == "__main__":
    asyncio.run(main())
