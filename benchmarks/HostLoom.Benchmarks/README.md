# HostLoom benchmarks

BenchmarkDotNet comparisons for the WebSocket envelope codecs. Run them from the repository root
on an otherwise idle machine:

```text
dotnet run --project benchmarks/HostLoom.Benchmarks -c Release -- --filter "*WebSocketProtocol*"
```

The encode and decode suites compare JSON, MessagePack, and protobuf-net for zero-byte, 256-byte,
and 4 KiB application payloads. `MemoryDiagnoser` reports managed allocations alongside throughput.

For a quick build-and-discovery smoke run rather than statistically meaningful results:

```text
dotnet run --project benchmarks/HostLoom.Benchmarks -c Release -- --job Dry --filter "*WebSocketProtocol*"
```

Do not treat a dry run, virtualized CI result, or one developer machine as a capacity claim. Run the
full job on deployment-like hardware and benchmark complete sessions separately before choosing
production connection and queue limits.
