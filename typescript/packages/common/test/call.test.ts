import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import { callDetails, decodeRequest, decodeResponse, requestPayload, responsePayload } from "../src/index.js";

test("shared call facts round trip null, zero and separate source details", () => {
 const vector = JSON.parse(readFileSync(new URL("../../../../../conformance/vectors/common-call.json", import.meta.url), "utf8"));
 for (const [name, decode, encode] of [["request", decodeRequest, requestPayload], ["response", decodeResponse, responsePayload]] as const) {
  const payload = { ...vector[name], ...vector.details };
  const facts = decode(payload);
  assert.deepEqual(encode(facts), vector[name]);
  assert.deepEqual(callDetails(payload), vector.details);
 }
});
test("invalid shared facts fail before recording or rendering", () => {
 assert.throws(() => decodeRequest({input: null, input_artifact_id: "artifact"}), /ambiguous/);
 assert.throws(() => decodeResponse({usage: {input_tokens: -1}}), /usage/);
 assert.throws(() => decodeResponse({error: {type: "Error", message: 7}}), /message/);
 assert.throws(() => decodeRequest({model: {id: ""}}), /model/);
});
