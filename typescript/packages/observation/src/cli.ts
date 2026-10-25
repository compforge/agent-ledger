#!/usr/bin/env node
import { readFile, writeFile } from "node:fs/promises";
import { parseArgs } from "node:util";
import { renderHTML } from "./render.js";
import type { SessionView } from "./model.js";

try {
  const {values, positionals} = parseArgs({allowPositionals: true, options: {
    output: {type: "string", short: "o"}, title: {type: "string"}, help: {type: "boolean", short: "h"},
  }});
  if (values.help) {
    process.stdout.write("Usage: session-view <session-view.json> [-o report.html] [--title title]\n");
  } else {
    if (positionals.length !== 1) throw new Error("expected one SessionView JSON file; see --help");
    const document = JSON.parse(await readFile(positionals[0]!, "utf8")) as SessionView;
    const html = renderHTML(document, values.title ?? "Agent session");
    if (values.output) await writeFile(values.output, html, "utf8");
    else process.stdout.write(html);
  }
} catch (error) {
  process.stderr.write(`session-view: ${error instanceof Error ? error.message : String(error)}\n`);
  process.exitCode = 1;
}
