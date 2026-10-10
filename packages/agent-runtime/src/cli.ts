import { readFile, writeFile, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { capture } from "./transport.js";
import { environment, MODEL, VERSION, BINARY } from "./isolation.js";
import { extract, transcript, ResponseError, type Role } from "./response.js";
import { fixture, MODES, type Mode } from "./fixture.js";
import { scopedAuth, redact } from "./auth.js";

interface Request {
  role: Role;
  model: string;
  prompt: string;
  timeout_ms: number;
  attempt: number;
  fixture?: Mode;
}
function request(value: unknown): Request {
  if (typeof value !== "object" || value === null || Array.isArray(value))
    throw new Error("Invalid runtime request");
  const item = value as Record<string, unknown>;
  if (
    !["planner", "coder", "reviewer"].includes(String(item.role)) ||
    item.model !== MODEL ||
    typeof item.prompt !== "string" ||
    Buffer.byteLength(item.prompt) > 262144 ||
    !item.prompt ||
    typeof item.timeout_ms !== "number" ||
    !Number.isInteger(item.timeout_ms) ||
    item.timeout_ms < 1 ||
    item.timeout_ms > 120000 ||
    typeof item.attempt !== "number" ||
    !Number.isInteger(item.attempt) ||
    item.attempt < 1 ||
    (item.fixture !== undefined && !MODES.includes(item.fixture as Mode)) ||
    Object.keys(item).some(
      (key) =>
        ![
          "role",
          "model",
          "prompt",
          "timeout_ms",
          "attempt",
          "fixture",
        ].includes(key),
    )
  )
    throw new Error("Invalid runtime request");
  return item as unknown as Request;
}

async function main(): Promise<void> {
  if (process.argv[2] === "--fixture") {
    fixture(
      process.argv[3] as Role,
      process.argv[4] as Mode,
      Number(process.argv[5]),
    );
    return;
  }
  if (process.versions.node !== "24.15.0")
    throw new Error("Node version must be 24.15.0");
  const input = request(
    JSON.parse(await readFile(process.argv[2] ?? "", "utf8")) as unknown,
  );
  const root = await mkdtemp(join(tmpdir(), "agent-session-"));
  try {
    const env = await environment(root);
    const secrets = input.fixture ? [] : await scopedAuth(root);
    const version = await capture(
      BINARY,
      ["--version"],
      root,
      env,
      30000,
      4096,
    );
    if (version.exit_code !== 0 || version.stdout.trim() !== VERSION) {
      await writeFile("events.ndjson", "");
      await writeFile("stdout.log", version.stdout);
      await writeFile("stderr.log", version.stderr);
      await writeFile(
        "runtime.json",
        JSON.stringify({
          ...version,
          version: VERSION,
          model: input.model,
          node_version: process.versions.node,
          error_class: "permanent",
          retryable: false,
        }),
      );
      process.exitCode = 65;
      return;
    }
    const args = input.fixture
      ? [
          new URL("./cli.js", import.meta.url).pathname,
          "--fixture",
          input.role,
          input.fixture,
          String(input.attempt),
        ]
      : ["run", input.prompt, "--model", input.model, "--format", "json"];
    const result = await capture(
      input.fixture ? process.execPath : BINARY,
      args,
      root,
      env,
      input.timeout_ms,
    );
    result.stdout = redact(result.stdout, secrets);
    result.stderr = redact(result.stderr, secrets);
    const metadata = {
      ...result,
      stdout: undefined,
      stderr: undefined,
      version: VERSION,
      node_version: process.versions.node,
      model: input.model,
      role: input.role,
      retryable: false,
    };
    await writeFile("events.ndjson", result.stdout);
    await writeFile("stdout.log", result.stdout);
    await writeFile("stderr.log", result.stderr);
    const diagnostic = transcript(result.stdout);
    await writeFile(
      "event-diagnostics.json",
      JSON.stringify(diagnostic.diagnostics),
    );
    let text = diagnostic.text;
    try {
      if (result.error_class)
        throw new ResponseError(
          result.error_class,
          "Runtime transport interrupted",
        );
      const extracted = extract(result.stdout, input.role);
      text = extracted.text;
      if (result.exit_code !== 0)
        throw new ResponseError("permanent", "Nonzero CLI exit");
      await writeFile("response.json", JSON.stringify(extracted.response));
      if (input.role === "coder")
        await writeFile("patch.diff", String(extracted.response.unified_diff));
    } catch (error: unknown) {
      if (!(error instanceof ResponseError)) throw error;
      metadata.error_class = error.error_class;
      metadata.retryable = ["rate_limit", "transport"].includes(
        error.error_class,
      );
    }
    await writeFile("transcript.txt", text);
    await writeFile("runtime.json", JSON.stringify(metadata));
    process.exitCode = metadata.error_class
      ? metadata.retryable
        ? 75
        : 65
      : 0;
  } catch (error: unknown) {
    // Pre-invocation failures still produce sanitized, bounded attempt diagnostics.
    await writeFile(
      "runtime.json",
      JSON.stringify({
        version: VERSION,
        node_version: process.versions.node,
        model: input.model,
        role: input.role,
        error_class: "permanent",
        retryable: false,
        exception: error instanceof Error ? error.name : "UnknownError",
      }),
    );
    process.exitCode = 65;
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}
await main();
