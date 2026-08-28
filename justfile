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
