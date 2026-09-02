# HostLoom developer shortcuts. Run `just` to list them.

solution := "HostLoom.slnx"

# List the available recipes.
default:
    @just --list

# Restore the pinned local tools (CSharpier).
tools:
    dotnet tool restore

# Format every C# source file in place.
format: tools
    dotnet csharpier format .

# Fail when any C# source file is not formatted.
format-check: tools
    dotnet csharpier check .

# Restore the solution's package graph.
restore:
    dotnet restore {{ solution }}

# Build the solution.
build:
    dotnet build {{ solution }}

# Run the test suite.
test:
    dotnet test {{ solution }}

# Start the RabbitMQ and Kafka brokers used by the integration tests.
brokers-up:
    docker compose up -d --wait

# Stop the brokers and remove their volumes.
brokers-down:
    docker compose down -v

# Run the transport integration tests. Requires `just brokers-up` first; they skip otherwise.
test-integration:
    dotnet test tests/HostLoom.IntegrationTests/HostLoom.IntegrationTests.csproj -c Release

# Compare HostLoom, HybridCache, and FusionCache on process-local cache paths.
benchmark-cache-libraries:
    dotnet run --project benchmarks/HostLoom.Benchmarks -c Release -- --filter "*CacheLibrary*"

# Produce stable HostLoom-only cache and lock reports for the regression gate.
benchmark-cache-lock:
    dotnet run --project benchmarks/HostLoom.Benchmarks -c Release -- --job Short --exporters json --filter "HostLoom.Benchmarks.CachingBenchmarks.*" "HostLoom.Benchmarks.LockingBenchmarks.*"

# Fail when mean time or allocations exceed the committed cache/lock baseline by over 10%.
benchmark-cache-lock-check: benchmark-cache-lock
    python3 benchmarks/check_cache_lock_baseline.py

# Deliberately replace the cache/lock baseline after reviewing an intentional change.
benchmark-cache-lock-update: benchmark-cache-lock
    python3 benchmarks/check_cache_lock_baseline.py --update

# Compare Redis-backed caches and locks. Configure HOSTLOOM_BENCHMARK_REDIS when not local.
benchmark-redis:
    dotnet run --project benchmarks/HostLoom.Redis.Benchmarks -c Release -- --filter "*"
