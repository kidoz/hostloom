import { readFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

// Node runs this file directly through type stripping, so it must stay dependency-free and
// use only erasable syntax: the release workflow validates a tag before `npm ci` or a build.

const packageDirectory = resolve(dirname(fileURLToPath(import.meta.url)), "..");

export function validateRelease(tag: string, packageJson: string, changelog: string): string {
    const prereleaseIdentifier = "(?:0|[1-9]\\d*|[0-9]*[A-Za-z-][0-9A-Za-z-]*)";
    const tagPattern = new RegExp(
        `^websocket-client-v(0|[1-9]\\d*)\\.(0|[1-9]\\d*)\\.(0|[1-9]\\d*)` +
            `(?:-${prereleaseIdentifier}(?:\\.${prereleaseIdentifier})*)?$`,
    );
    const match = tagPattern.exec(tag);
    if (match === null) {
        throw new Error(
            `Release tag '${tag}' is invalid. Expected websocket-client-vMAJOR.MINOR.PATCH with an optional prerelease suffix.`,
        );
    }

    const version = tag.slice("websocket-client-v".length);
    const packageVersion = (JSON.parse(packageJson) as { readonly version?: unknown }).version;
    if (packageVersion !== version) {
        throw new Error(
            `Release tag version '${version}' does not match package.json version '${String(packageVersion)}'.`,
        );
    }

    if (!changelog.includes(`## [${version}]`)) {
        throw new Error(`CHANGELOG.md has no '## [${version}]' section.`);
    }

    return version;
}

async function main(): Promise<void> {
    const tag = process.argv[2];
    if (tag === undefined) {
        throw new Error("Pass the WebSocket client release tag as the first argument.");
    }

    const [packageJson, changelog] = await Promise.all([
        readFile(resolve(packageDirectory, "package.json"), "utf8"),
        readFile(resolve(packageDirectory, "CHANGELOG.md"), "utf8"),
    ]);
    process.stdout.write(`${validateRelease(tag, packageJson, changelog)}\n`);
}

const invokedPath = process.argv[1];
if (invokedPath !== undefined && pathToFileURL(resolve(invokedPath)).href === import.meta.url) {
    main().catch((error: unknown) => {
        process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
        process.exitCode = 1;
    });
}
