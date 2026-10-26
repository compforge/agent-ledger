import assert from "node:assert/strict";
import { execFileSync, spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";

test("CLI renders shared JSON and rejects invalid evidence without output", () => {
  const directory = mkdtempSync(join(tmpdir(), "session-view-cli-"));
  try {
    const cli = fileURLToPath(new URL("../src/cli.js", import.meta.url));
    const source = fileURLToPath(new URL("../../../../../conformance/vectors/session-view.json", import.meta.url));
    const output = join(directory, "report.html");
    execFileSync(process.execPath, [cli, source, "--output", output, "--title", "Shared execution"]);
    assert.match(readFileSync(output, "utf8"), /Shared execution/);
    const job = fileURLToPath(new URL("../../../../../conformance/vectors/job-input.json", import.meta.url));
    execFileSync(process.execPath, [cli, job, "--output", output]);
    assert.match(readFileSync(output, "utf8"), /outside Job snapshot/);
    const invalid = join(directory, "invalid.json");
    writeFileSync(invalid, JSON.stringify({schema_version: "unknown", runs: []}));
    const result = spawnSync(process.execPath, [cli, invalid], {encoding: "utf8"});
    assert.equal(result.status, 1);
    assert.match(result.stderr, /unsupported View schema/);
    assert.equal(result.stdout, "");
  } finally { rmSync(directory, {recursive: true, force: true}); }
});
