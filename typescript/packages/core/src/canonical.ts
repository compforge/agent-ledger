import { createHash } from "node:crypto";

import type { ProposedEvent } from "./types.js";

export function canonicalize(value: unknown): string {
  if (value === null || typeof value === "boolean") return JSON.stringify(value);
  if (typeof value === "number") {
    if (!Number.isFinite(value)) throw new TypeError("canonical JSON requires finite numbers");
    return JSON.stringify(value);
  }
  if (typeof value === "string") {
    assertNoLoneSurrogate(value);
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) return `[${value.map(canonicalize).join(",")}]`;
  if (typeof value === "object") {
    const object = value as Record<string, unknown>;
    return `{${Object.keys(object)
      .sort()
      .map((key) => {
        assertNoLoneSurrogate(key);
        return `${JSON.stringify(key)}:${canonicalize(object[key])}`;
      })
      .join(",")}}`;
  }
  throw new TypeError(`value of type ${typeof value} is not JSON`);
}

export function canonicalAppendDigest(events: readonly ProposedEvent[]): string {
  return createHash("sha256").update(canonicalize(events)).digest("hex");
}

function assertNoLoneSurrogate(value: string): void {
  for (let index = 0; index < value.length; index += 1) {
    const code = value.charCodeAt(index);
    if (code >= 0xd800 && code <= 0xdbff) {
      const next = value.charCodeAt(index + 1);
      if (next < 0xdc00 || next > 0xdfff) throw new TypeError("canonical JSON rejects lone surrogates");
      index += 1;
    } else if (code >= 0xdc00 && code <= 0xdfff) {
      throw new TypeError("canonical JSON rejects lone surrogates");
    }
  }
}
