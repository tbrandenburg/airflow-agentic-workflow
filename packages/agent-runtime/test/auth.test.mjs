import assert from "node:assert/strict";
import { test } from "node:test";
import { mkdtemp, writeFile, readFile, stat, rm } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { scopedAuth, redact, PROVIDER } from "../dist/auth.js";
test("one-provider auth is disposable, private and redacted; mixed providers rejected", async () => {
  const root = await mkdtemp(join(tmpdir(), "auth-test-"));
  try {
    const credential = {
      type: "oauth",
      refresh: "synthetic-refresh",
      access: "synthetic-access",
      expires: 0,
    };
    const source = join(root, "provider.json");
    const original = JSON.stringify({ [PROVIDER]: credential });
    await writeFile(source, original);
    const secrets = await scopedAuth(root, source);
    const copy = join(root, "session/data/opencode/auth.json");
    assert.equal((await stat(copy)).mode & 0o777, 0o600);
    assert.equal(await readFile(copy, "utf8"), original);
    assert.equal(
      redact("synthetic-refresh synthetic-access Bearer sensitive", secrets),
      "[REDACTED] [REDACTED] Bearer [REDACTED]",
    );
    await writeFile(copy, "disposable refresh");
    assert.equal(await readFile(source, "utf8"), original);
    await writeFile(
      source,
      JSON.stringify({ [PROVIDER]: credential, unapproved: credential }),
    );
    await assert.rejects(scopedAuth(root, source), /not approved/);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
