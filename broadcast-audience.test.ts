import test from "node:test";
import assert from "node:assert/strict";
import { broadcastAudience } from "./broadcast-audience.ts";
import { isMessageAudience } from "./broker/protocol.ts";
import { AUDIENCE_GROUP_MAX_LENGTH } from "./types.ts";

test("a broadcast audience names the group as the sender chose it", () => {
  assert.deepEqual(broadcastAudience({}), { kind: "broadcast" });
  assert.deepEqual(broadcastAudience({ to: " box:* ", app: "Pi*" }), { kind: "broadcast", group: "box:*, sessions in Pi*" });
});

test("a long directory is shortened from the front so receivers accept the label and the pattern survives", () => {
  const cwd = `/Users/someone/${"nested/".repeat(60)}project`;
  const audience = broadcastAudience({ to: "flightdeck:*", cwd });
  assert.equal(isMessageAudience(audience), true);
  assert.ok(audience.group!.length <= AUDIENCE_GROUP_MAX_LENGTH);
  assert.match(audience.group!, /^flightdeck:\*, sessions in ….*\/project$/);
});

test("even an overlong pattern still yields a label receivers accept", () => {
  const audience = broadcastAudience({ to: "x".repeat(AUDIENCE_GROUP_MAX_LENGTH * 2) });
  assert.equal(isMessageAudience(audience), true);
});
