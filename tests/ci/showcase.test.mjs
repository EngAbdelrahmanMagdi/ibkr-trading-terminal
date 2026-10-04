import test from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { spawnSync } from "node:child_process";
import path from "node:path";

const root = path.resolve(import.meta.dirname, "../..");
test("static bundle has no trading network or executable scripts", async () => {
  const result = spawnSync(process.execPath, ["apps/showcase/build.mjs"], { cwd: root, encoding: "utf8", env: { ...process.env, SHOWCASE_PREFIX: "/showcase", SHOWCASE_OUTPUT: path.join(root, ".artifacts/showcase-test") } });
  assert.equal(result.status, 0, result.stderr);
  const html = await readFile(path.join(root, ".artifacts/showcase-test/showcase/index.html"), "utf8");
  assert.doesNotMatch(html, /<script|autoplay|noindex|nofollow|wss?:\/\/|localhost|127\.0\.0\.1|__TRANSCRIPT__/i);
  assert.match(html, /Full stack running locally in simulated\/MOCK mode/);
  assert.match(html, /src="\/showcase\/terminal.mp4"/);
  assert.match(html, /<video controls/);
  const headers = await readFile(path.join(root, ".artifacts/showcase-test/_headers"), "utf8");
  assert.match(headers, /script-src 'none'; connect-src 'none'/);
});
test("build rejects unsafe prefix and source output", () => {
  for (const extra of [{ SHOWCASE_PREFIX: "/../escape" }, { SHOWCASE_OUTPUT: root }]) {
    const result = spawnSync(process.execPath, ["apps/showcase/build.mjs"], { cwd: root, env: { ...process.env, ...extra } });
    assert.notEqual(result.status, 0);
  }
});
