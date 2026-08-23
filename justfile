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
