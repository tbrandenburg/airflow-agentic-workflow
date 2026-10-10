import assert from "node:assert/strict";
import { test } from "node:test";
import { mkdtemp, readFile, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawnSync } from "node:child_process";
import { capture } from "../dist/transport.js";
import { environment, MODEL } from "../dist/isolation.js";
import { extract } from "../dist/response.js";

const cli = new URL("../dist/cli.js", import.meta.url).pathname;
test("fixed argv, closed stdin and isolated environment", async () => {
  const root = await mkdtemp(join(tmpdir(), "agent-test-"));
  try {
    const env = await environment(root);
    assert.equal(env.AWS_SECRET_ACCESS_KEY, undefined);
    assert.equal(env.OPENCODE_CONFIG, undefined);
    assert.equal(env.HTTPS_PROXY, "http://model-egress:8080");
    assert.equal(env.HTTP_PROXY, undefined);
    const prompt = "literal $(touch forbidden) ; --session bad";
    const code =
      'let n=0;process.stdin.on("data",d=>n+=d.length);process.stdin.on("end",()=>console.log(JSON.stringify({args:process.argv.slice(1),stdin:n,home:process.env.HOME})))';
    const result = await capture(
      process.execPath,
      ["-e", code, "run", prompt, "--model", MODEL, "--format", "json"],
      root,
      env,
      5000,
    );
    const actual = JSON.parse(result.stdout);
    assert.deepEqual(actual.args, [
      "run",
      prompt,
      "--model",
      MODEL,
      "--format",
      "json",
    ]);
    assert.equal(actual.stdin, 0);
    assert.equal(actual.home, join(root, "session"));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

for (const role of ["planner", "coder", "reviewer"]) {
  test(`compiled CLI success ${role}`, async () => {
    const root = await mkdtemp(join(tmpdir(), "agent-test-"));
    try {
      await writeFile(
        join(root, "request.json"),
        JSON.stringify({
          role,
          model: MODEL,
          prompt: "test",
          timeout_ms: 3000,
          attempt: 1,
          fixture: "success",
        }),
      );
      const result = spawnSync(process.execPath, [cli, "request.json"], {
        cwd: root,
        timeout: 60000,
      });
      assert.equal(result.status, 0, result.stderr.toString());
      const response = JSON.parse(
        await readFile(join(root, "response.json"), "utf8"),
      );
      assert.equal(response.role, role);
      if (role === "coder")
        assert.match(
          await readFile(join(root, "patch.diff"), "utf8"),
          /^diff --git /,
        );
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
}
for (const [mode, classification, attempt, status] of [
  ["transient", "rate_limit", 1, 75],
  ["transient", null, 2, 0],
  ["permanent", "permanent", 1, 65],
  ["timeout", "timeout", 1, 65],
  ["malformed", "invalid_output", 1, 65],
  ["schema-invalid", "invalid_output", 1, 65],
  ["missing-patch", "invalid_output", 1, 65],
  ["oversized", "oversized", 1, 65],
]) {
  test(`failure evidence ${mode} attempt ${attempt}`, async () => {
    const root = await mkdtemp(join(tmpdir(), "agent-test-"));
    try {
      await writeFile(
        join(root, "request.json"),
        JSON.stringify({
          role: mode === "missing-patch" ? "coder" : "planner",
          model: MODEL,
          prompt: "test",
          timeout_ms: 3000,
          attempt,
          fixture: mode,
        }),
      );
      const result = spawnSync(process.execPath, [cli, "request.json"], {
        cwd: root,
        timeout: 60000,
      });
      assert.equal(result.status, status, result.stderr.toString());
      const metadata = JSON.parse(
        await readFile(join(root, "runtime.json"), "utf8"),
      );
      assert.equal(metadata.error_class, classification);
      assert.ok(
        (await readFile(join(root, "events.ndjson"))).length ||
          mode === "timeout",
      );
      const stderr = await readFile(join(root, "stderr.log"), "utf8");
      if (mode === "timeout") {
        const pid = Number(stderr.match(/descendant=(\d+)/)?.[1]);
        assert.ok(pid > 0);
        // Killed descendants can briefly remain zombies until adopted/reaped.
        try {
          const stat = await readFile(`/proc/${pid}/stat`, "utf8");
          assert.equal(stat.split(" ")[2], "Z");
        } catch (error) {
          if (error.code !== "ENOENT") throw error;
        }
      }
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
}
test("interrupted, role-mismatched and raw application JSON are rejected", () => {
  for (const events of [
    "{}",
    "{bad",
    JSON.stringify({ type: "text", part: { text: "{}" } }),
  ]) {
    assert.throws(() => extract(events, "planner"), {
      error_class: "invalid_output",
    });
  }
});
