using System.Diagnostics;
using System.IO.Compression;
using System.Xml.Linq;
using Xunit;

namespace HostLoom.Analyzers.Tests;

public sealed class AnalyzerPackageTests
{
    [Fact]
    public async Task Package_loads_without_runtime_assets_or_dependencies()
    {
        string root = FindRepositoryRoot();
        string packageDirectory = Path.Combine(
            Path.GetTempPath(),
            "hostloom-analyzer-packages-" + Guid.NewGuid().ToString("N")
        );
        string consumerDirectory = Path.Combine(
            Path.GetTempPath(),
            "hostloom-analyzer-consumer-" + Guid.NewGuid().ToString("N")
        );
        Directory.CreateDirectory(packageDirectory);
        Directory.CreateDirectory(consumerDirectory);

        try
        {
            foreach (
                string project in new[]
                {
                    "HostLoom.Pipelines/HostLoom.Pipelines.csproj",
                    "HostLoom/HostLoom.csproj",
                    "HostLoom.Analyzers/HostLoom.Analyzers.csproj",
                }
            )
            {
                CommandResult pack = await RunDotnetAsync(
                        root,
                        [
                            "pack",
                            Path.Combine(root, "src", project),
                            "--configuration",
                            "Release",
                            "--artifacts-path",
                            Path.Combine(packageDirectory, "build"),
                            "--output",
                            packageDirectory,
                            "-p:Version=0.0.0-test",
                        ]
                    )
                    .ConfigureAwait(true);
                AssertCommandSucceeded(pack);
            }

            string package = Assert.Single(
                Directory.GetFiles(packageDirectory, "HostLoom.Analyzers.*.nupkg")
            );
            Assert.Empty(Directory.GetFiles(packageDirectory, "HostLoom.Analyzers.*.snupkg"));
            AssertPackageLayout(package);

            string projectPath = await WriteConsumerAsync(consumerDirectory, packageDirectory)
                .ConfigureAwait(true);
            CommandResult restore = await RunDotnetAsync(
                    consumerDirectory,
                    [
                        "restore",
                        projectPath,
                        "--configfile",
                        Path.Combine(consumerDirectory, "NuGet.config"),
                    ]
                )
                .ConfigureAwait(true);
            AssertCommandSucceeded(restore);

            CommandResult build = await RunDotnetAsync(
                    consumerDirectory,
                    ["build", projectPath, "--configuration", "Release", "--no-restore"]
                )
                .ConfigureAwait(true);
            AssertCommandSucceeded(build);
            Assert.Contains("warning HLM0001", build.Output, StringComparison.Ordinal);
        }
        finally
        {
            Directory.Delete(packageDirectory, recursive: true);
            Directory.Delete(consumerDirectory, recursive: true);
        }
    }

    private static void AssertPackageLayout(string packagePath)
    {
        using ZipArchive archive = ZipFile.OpenRead(packagePath);
        string[] entries = archive.Entries.Select(entry => entry.FullName).ToArray();
        Assert.Equal(
            ["analyzers/dotnet/cs/HostLoom.Analyzers.dll"],
            entries
                .Where(entry => entry.EndsWith(".dll", StringComparison.OrdinalIgnoreCase))
                .ToArray()
        );
        Assert.Contains("README.md", entries);
        Assert.DoesNotContain(entries, entry => entry.StartsWith("lib/", StringComparison.Ordinal));
        Assert.DoesNotContain(entries, entry => entry.StartsWith("ref/", StringComparison.Ordinal));
        Assert.DoesNotContain(
            entries,
            entry => entry.StartsWith("build/", StringComparison.Ordinal)
        );
        Assert.DoesNotContain(
            entries,
            entry => entry.StartsWith("buildTransitive/", StringComparison.Ordinal)
        );

        ZipArchiveEntry nuspec = Assert.Single(
            archive.Entries,
            entry => entry.FullName.EndsWith(".nuspec", StringComparison.OrdinalIgnoreCase)
        );
        using Stream stream = nuspec.Open();
        XDocument document = XDocument.Load(stream);
        Assert.DoesNotContain(
            document.Descendants(),
            element => element.Name.LocalName == "dependency"
        );
    }

    private static async Task<string> WriteConsumerAsync(
        string consumerDirectory,
        string packageDirectory
    )
    {
        string projectPath = Path.Combine(consumerDirectory, "Consumer.csproj");
        string sourcePath = Path.Combine(consumerDirectory, "Consumer.cs");
        string nugetConfigPath = Path.Combine(consumerDirectory, "NuGet.config");

        var project = new XDocument(
            new XElement(
                "Project",
                new XAttribute("Sdk", "Microsoft.NET.Sdk"),
                new XElement(
                    "PropertyGroup",
                    new XElement("TargetFramework", "net10.0"),
                    new XElement("Nullable", "enable"),
                    new XElement("ImplicitUsings", "enable")
                ),
                new XElement(
                    "ItemGroup",
                    new XElement(
                        "FrameworkReference",
                        new XAttribute("Include", "Microsoft.AspNetCore.App")
                    ),
                    new XElement(
                        "PackageReference",
                        new XAttribute("Include", "HostLoom"),
                        new XAttribute("Version", "0.0.0-test")
                    ),
                    new XElement(
                        "PackageReference",
                        new XAttribute("Include", "HostLoom.Analyzers"),
                        new XAttribute("Version", "0.0.0-test"),
                        new XAttribute("PrivateAssets", "all")
                    )
                )
            )
        );
        var nugetConfig = new XDocument(
            new XElement(
                "configuration",
                new XElement(
                    "packageSources",
                    new XElement("clear"),
                    new XElement(
                        "add",
                        new XAttribute("key", "local"),
                        new XAttribute("value", packageDirectory)
                    )
                )
            )
        );

        await File.WriteAllTextAsync(
                projectPath,
                project.ToString(),
                TestContext.Current.CancellationToken
            )
            .ConfigureAwait(true);
        await File.WriteAllTextAsync(
                nugetConfigPath,
                nugetConfig.ToString(),
                TestContext.Current.CancellationToken
            )
            .ConfigureAwait(true);
        await File.WriteAllTextAsync(
                sourcePath,
                ConsumerSource,
                TestContext.Current.CancellationToken
            )
            .ConfigureAwait(true);
        return projectPath;
    }

    private static async Task<CommandResult> RunDotnetAsync(
        string workingDirectory,
        IReadOnlyList<string> arguments
    )
    {
        using var process = new Process();
        process.StartInfo = new ProcessStartInfo("dotnet")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            WorkingDirectory = workingDirectory,
        };
        process.StartInfo.Environment["DOTNET_CLI_DO_NOT_USE_MSBUILD_SERVER"] = "1";
        foreach (string argument in arguments)
        {
            process.StartInfo.ArgumentList.Add(argument);
        }

        process.Start();
        Task<string> outputTask = process.StandardOutput.ReadToEndAsync();
        Task<string> errorTask = process.StandardError.ReadToEndAsync();
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        using CancellationTokenSource cancellation =
            CancellationTokenSource.CreateLinkedTokenSource(
                timeout.Token,
                TestContext.Current.CancellationToken
            );
        await process.WaitForExitAsync(cancellation.Token).ConfigureAwait(true);
        return new CommandResult(
            process.ExitCode,
            await outputTask.ConfigureAwait(true) + await errorTask.ConfigureAwait(true)
        );
    }

    private static void AssertCommandSucceeded(CommandResult result) =>
        Assert.True(result.ExitCode == 0, result.Output);

    private static string FindRepositoryRoot()
    {
        DirectoryInfo? directory = new(AppContext.BaseDirectory);
        while (directory is not null)
        {
            if (File.Exists(Path.Combine(directory.FullName, "HostLoom.slnx")))
            {
                return directory.FullName;
            }

            directory = directory.Parent;
        }

        throw new DirectoryNotFoundException("Could not locate HostLoom.slnx.");
    }

    private const string ConsumerSource = """
        using System.Threading;
        using System.Threading.Tasks;
        using HostLoom;

        internal sealed record Response(string Text);
        internal sealed record Request : IRequest<Response>;

        internal static class Consumer
        {
            public static async Task SendAsync(
                IRequestClient<Request, Response> client,
                CancellationToken cancellationToken)
            {
                await client.GetResponseAsync("requests", new Request());
            }
        }
        """;

    private sealed class CommandResult(int exitCode, string output)
    {
        public int ExitCode { get; } = exitCode;

        public string Output { get; } = output;
    }
}
