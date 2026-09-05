#!/usr/bin/env python3
"""Pack locally or verify supplied packages using fresh consumers outside the repository.

No package is published. Output, consumer sources and per-command logs are retained in a new
OS temporary directory. --runtime additionally publishes and executes the existing AOT sample
against the packed runtime, with no generator project reference.
"""

import argparse
import json
import os
import re
import shutil
import subprocess
import tempfile
import xml.etree.ElementTree as ET
import zipfile
from collections.abc import Mapping, Sequence
from pathlib import Path
from xml.sax.saxutils import escape


def require(condition: object, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--packages", type=Path, help="Existing package directory; omit to build and pack."
    )
    parser.add_argument("--version", default="0.0.0-composition-check")
    parser.add_argument("--runtime", help="Publish and execute the AOT sample for this local RID.")
    args = parser.parse_args()
    require(
        re.fullmatch(
            r"[0-9]+\.[0-9]+\.[0-9]+(?:-[A-Za-z0-9.-]+)?(?:\+[A-Za-z0-9.-]+)?", args.version
        ),
        "Invalid package version",
    )
    repo = Path(__file__).resolve().parents[1]
    work = Path(tempfile.mkdtemp(prefix="hostloom-composition-package-")).resolve()
    print(f"Evidence directory: {work}", flush=True)
    packages = args.packages.resolve() if args.packages else work / "packages"
    packages.mkdir(parents=True, exist_ok=True)
    sdk = json.loads((repo / "global.json").read_text())["sdk"]
    (work / "global.json").write_text(json.dumps({"sdk": sdk}))
    package_versions = ET.parse(repo / "Directory.Packages.props")
    di_version = next(
        item.attrib["Version"]
        for item in package_versions.iter("PackageVersion")
        if item.attrib["Include"] == "Microsoft.Extensions.DependencyInjection"
    )
    config = work / "nuget.config"
    config.write_text(f'''<configuration><packageSources><clear />
        <add key="local" value="{escape(str(packages))}" />
        <add key="nuget.org" value="https://api.nuget.org/v3/index.json" />
        </packageSources><packageSourceMapping><clear />
        <packageSource key="local"><package pattern="HostLoom.*" /></packageSource>
        <packageSource key="nuget.org"><package pattern="*" /></packageSource>
        </packageSourceMapping></configuration>''')
    consumer_env = dict(os.environ, NUGET_PACKAGES=str(work / "cache"))

    def run(
        label: str,
        command: Sequence[str],
        cwd: Path = repo,
        env: Mapping[str, str] | None = None,
        diagnostic: str | None = None,
    ) -> str:
        print(label, flush=True)
        result = subprocess.run(
            command,
            cwd=cwd,
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=600,
        )
        (work / f"{label}.log").write_text(result.stdout)
        if diagnostic:
            require(
                result.returncode != 0 and re.search(r"error " + diagnostic + r"\b", result.stdout),
                f"Expected {diagnostic}; inspect {work / (label + '.log')}",
            )
            require(
                "CS8785" not in result.stdout and "CS9057" not in result.stdout,
                "Generator load/host failure",
            )
        else:
            require(result.returncode == 0, f"{label} failed; inspect {work / (label + '.log')}")
        return result.stdout

    if not args.packages:
        for package in ("HostLoom.Composition", "HostLoom.Composition.Testing"):
            run(
                "pack-" + package,
                [
                    "dotnet",
                    "pack",
                    str(repo / "src" / package / (package + ".csproj")),
                    "--configuration",
                    "Release",
                    "--output",
                    str(packages),
                    "-p:Version=" + args.version,
                ],
            )

    def inspect_package(package: str, dependencies: set[str]) -> None:
        archive = packages / f"{package}.{args.version}.nupkg"
        require(archive.is_file(), f"Missing {archive}")
        with zipfile.ZipFile(archive) as packed:
            names = packed.namelist()
            spec = ET.fromstring(packed.read(package + ".nuspec"))
            found = {node.attrib["id"] for node in spec.iter() if node.tag.endswith("}dependency")}
            require(found == dependencies, f"Unexpected {package} dependencies: {found}")
            require(f"lib/net10.0/{package}.dll" in names, "Missing runtime assembly")
            if package == "HostLoom.Composition":
                require(
                    "analyzers/dotnet/cs/HostLoom.Composition.Generators.dll" in names,
                    "Missing bundled generator",
                )
                require(
                    "buildTransitive/HostLoom.Composition.props" in names,
                    "Missing compiler property props",
                )
                dlls = {name for name in names if name.endswith(".dll")}
                require(
                    dlls
                    == {
                        "lib/net10.0/HostLoom.Composition.dll",
                        "analyzers/dotnet/cs/HostLoom.Composition.Generators.dll",
                    },
                    f"Unexpected shipped assemblies: {dlls}",
                )
            (work / (package + ".contents.txt")).write_text("\n".join(names))

    inspect_package(
        "HostLoom.Composition", {"Microsoft.Extensions.DependencyInjection.Abstractions"}
    )
    inspect_package("HostLoom.Composition.Testing", {"HostLoom.Composition"})

    def project(directory: Path, references: Sequence[tuple[str, str]], extra: str = "") -> Path:
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / "Consumer.csproj"
        refs = "\n".join(
            f'<PackageReference Include="{package}" Version="{version}" />'
            for package, version in references
        )
        path.write_text(f"""<Project Sdk="Microsoft.NET.Sdk"><PropertyGroup>
            <TargetFramework>net10.0</TargetFramework><LangVersion>14.0</LangVersion>
            <OutputType>Exe</OutputType><ImplicitUsings>enable</ImplicitUsings><Nullable>enable</Nullable>
            <TreatWarningsAsErrors>true</TreatWarningsAsErrors><EmitCompilerGeneratedFiles>true</EmitCompilerGeneratedFiles>
            <CompilerGeneratedFilesOutputPath>obj/generated</CompilerGeneratedFilesOutputPath>{extra}
            </PropertyGroup><ItemGroup>{refs}</ItemGroup></Project>""")
        return path

    consumer = work / "application"
    app = project(consumer, [("HostLoom.Composition", args.version)])
    (consumer / "Composition").mkdir()
    rules = consumer / "Composition" / "Rules.cs"
    valid = """using HostLoom.Composition;
namespace CatalogApplication;
internal static partial class CatalogComposition
{
    [CompositionRules(nameof(CreatePlan))]
    private static void Declare(CompositionRuleBuilder rules)
    {
        rules.AddClasses().AssignableTo(typeof(ICatalog<>)).AsSelfWithInterfaces()
            .WithScopedLifetime().ExpectOne().ExpectExactly(2);
        rules.AddOpenGeneric(typeof(IRepository<>), typeof(Repository<>)).WithScopedLifetime().ExpectOne();
    }
    public static partial CompositionPlan CreatePlan();
}
internal interface ICatalog<T> { }
internal abstract class CatalogBase<T> : ICatalog<T> { }
internal sealed class Catalog : CatalogBase<string> { public Catalog() { } }
internal sealed class Inventory : CatalogBase<int> { public Inventory() { } }
internal interface IRepository<T> { }
internal sealed class Repository<T> : IRepository<T> { public Repository() { } }
"""
    rules.write_text(valid)
    (consumer / "Program.cs").write_text("""using CatalogApplication;
using Microsoft.Extensions.DependencyInjection;
var plan = CatalogComposition.CreatePlan();
if (plan.Probe().Registrations.Count != 5 || plan.Probe().Registrations.Any(entry => entry.Origin.FilePath != "Composition/Rules.cs"))
    throw new InvalidOperationException("Invalid generated registrations or package-provided relative origins.");
var services = new ServiceCollection();
if (plan.ApplyTo(services).Probe().Count != 5) throw new InvalidOperationException("Application failed.");
Console.WriteLine("Packed composition application passed.");
""")
    run(
        "restore-application",
        ["dotnet", "restore", str(app), "--configfile", str(config)],
        consumer,
        consumer_env,
    )
    run(
        "build-application",
        ["dotnet", "build", str(app), "-c", "Release", "--no-restore"],
        consumer,
        consumer_env,
    )
    run(
        "run-application",
        ["dotnet", str(consumer / "bin/Release/net10.0/Consumer.dll")],
        consumer,
        consumer_env,
    )
    assets = json.loads((consumer / "obj/project.assets.json").read_text())
    found = {name.split("/")[0] for name in assets["libraries"]}
    require(
        found == {"HostLoom.Composition", "Microsoft.Extensions.DependencyInjection.Abstractions"},
        f"Unexpected application graph: {found}",
    )
    output_dlls = {path.name for path in (consumer / "bin/Release/net10.0").glob("*.dll")}
    require(
        not any(
            "CodeAnalysis" in name or "Generators" in name or "Diagnostics" in name
            for name in output_dlls
        ),
        "Compiler or diagnostics dependency leaked to application output",
    )
    generated = list((consumer / "obj/generated").rglob("*.g.cs"))
    require(len(generated) == 1, "Expected exactly one generated plan")
    require(str(work) not in generated[0].read_text(), "Absolute checkout root leaked")
    rules.write_text(valid.replace("ExpectExactly(2)", "ExpectExactly(99)"))
    run(
        "invalid-count",
        ["dotnet", "build", str(app), "-c", "Release", "--no-restore"],
        consumer,
        consumer_env,
        "HLM0014",
    )
    rules.write_text(
        valid
        + "\ninternal static partial class CatalogComposition { public static void ExecuteRules() => Declare(null!); }\n"
    )
    run(
        "invalid-runtime-use",
        ["dotnet", "build", str(app), "-c", "Release", "--no-restore"],
        consumer,
        consumer_env,
        "HLM0009",
    )
    rules.write_text(valid)
    run(
        "rebuild-application",
        ["dotnet", "build", str(app), "-c", "Release", "--no-restore"],
        consumer,
        consumer_env,
    )

    testing = work / "testing"
    test_app = project(testing, [("HostLoom.Composition.Testing", args.version)])
    # Reuse declarations to verify generator delivery through the optional helper's dependency too.
    (testing / "Composition").mkdir()
    (testing / "Composition/Rules.cs").write_text(valid)
    (testing / "Program.cs").write_text("""using CatalogApplication;
using HostLoom.Composition.Testing;
var first = CatalogComposition.CreatePlan();
var second = CatalogComposition.CreatePlan();
CompositionAssert.EquivalentRegistrations(first, second);
CompositionAssert.RegistrationSequence(first, second);
if (first.Probe().Registrations[0].Origin.FilePath != "Composition/Rules.cs") throw new InvalidOperationException("Transitive props missing.");
Console.WriteLine("Packed testing helpers passed.");
""")
    run(
        "restore-testing",
        ["dotnet", "restore", str(test_app), "--configfile", str(config)],
        testing,
        consumer_env,
    )
    run(
        "build-testing",
        ["dotnet", "build", str(test_app), "-c", "Release", "--no-restore"],
        testing,
        consumer_env,
    )
    run(
        "run-testing",
        ["dotnet", str(testing / "bin/Release/net10.0/Consumer.dll")],
        testing,
        consumer_env,
    )
    testing_assets = json.loads((testing / "obj/project.assets.json").read_text())
    require(
        {name.split("/")[0] for name in testing_assets["libraries"]}
        == {
            "HostLoom.Composition.Testing",
            "HostLoom.Composition",
            "Microsoft.Extensions.DependencyInjection.Abstractions",
        },
        "Unexpected testing consumer dependency graph",
    )

    if args.runtime:
        aot = work / "aot"
        native_app = project(
            aot,
            [
                ("HostLoom.Composition", args.version),
                ("Microsoft.Extensions.DependencyInjection", di_version),
            ],
            "<PublishAot>true</PublishAot><InvariantGlobalization>true</InvariantGlobalization>",
        )
        for source in (repo / "examples/HostLoom.Examples.CompositionAot").glob("*.cs"):
            shutil.copy2(source, aot / source.name)
        run(
            "restore-aot",
            ["dotnet", "restore", str(native_app), "-r", args.runtime, "--configfile", str(config)],
            aot,
            consumer_env,
        )
        run(
            "publish-aot",
            [
                "dotnet",
                "publish",
                str(native_app),
                "-c",
                "Release",
                "-r",
                args.runtime,
                "--no-restore",
                "--output",
                str(aot / "native"),
            ],
            aot,
            consumer_env,
        )
        executable = aot / "native" / ("Consumer.exe" if os.name == "nt" else "Consumer")
        run("run-aot", [str(executable)], aot, consumer_env)
        run(
            "restore-testing-aot",
            [
                "dotnet",
                "restore",
                str(test_app),
                "-r",
                args.runtime,
                "-p:PublishAot=true",
                "--configfile",
                str(config),
            ],
            testing,
            consumer_env,
        )
        run(
            "publish-testing-aot",
            [
                "dotnet",
                "publish",
                str(test_app),
                "-c",
                "Release",
                "-r",
                args.runtime,
                "-p:PublishAot=true",
                "--no-restore",
                "--output",
                str(testing / "native"),
            ],
            testing,
            consumer_env,
        )
        run(
            "run-testing-aot",
            [str(testing / "native" / ("Consumer.exe" if os.name == "nt" else "Consumer"))],
            testing,
            consumer_env,
        )
    summary = {
        "version": args.version,
        "packages": str(packages),
        "application_dependencies": sorted(found),
        "negative_diagnostics": ["HLM0014", "HLM0009"],
        "testing_consumer": "passed",
        "aot_runtime": args.runtime,
    }
    (work / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print("Packed composition verification passed. " + str(work / "summary.json"), flush=True)


if __name__ == "__main__":
    main()
