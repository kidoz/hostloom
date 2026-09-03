import assert from "node:assert/strict";
import { test } from "vitest";

import { validateRelease } from "../scripts/check-release.ts";

const packageJson = JSON.stringify({ version: "0.1.0" });
const changelog = "# Changelog\n\n## [0.1.0] - 2026-09-03\n";

test("release validation accepts the independent client tag and returns its version", () => {
    assert.equal(validateRelease("websocket-client-v0.1.0", packageJson, changelog), "0.1.0");
});

test("release validation accepts a strict semantic prerelease", () => {
    assert.equal(
        validateRelease(
            "websocket-client-v0.1.0-rc.1",
            JSON.stringify({ version: "0.1.0-rc.1" }),
            "## [0.1.0-rc.1] - 2026-09-03\n",
        ),
        "0.1.0-rc.1",
    );
});

test.each([
    "v0.1.0",
    "websocket-client-v01.0.0",
    "websocket-client-v0.1",
    "websocket-client-v0.1.0-",
])("release validation rejects invalid tag %s", (tag) => {
    assert.throws(() => validateRelease(tag, packageJson, changelog), /Release tag/);
});

test("release validation rejects a numeric prerelease identifier with a leading zero", () => {
    assert.throws(
        () =>
            validateRelease(
                "websocket-client-v0.1.0-01",
                JSON.stringify({ version: "0.1.0-01" }),
                "## [0.1.0-01]\n",
            ),
        /Release tag/,
    );
});

test("release validation rejects version and changelog mismatches", () => {
    assert.throws(
        () => validateRelease("websocket-client-v0.2.0", packageJson, "## [0.2.0]\n"),
        /does not match package.json/,
    );
    assert.throws(
        () => validateRelease("websocket-client-v0.1.0", packageJson, "# Changelog\n"),
        /has no '## \[0.1.0\]' section/,
    );
});
