import { mkdtemp, rm, access } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { capture } from "../dist/transport.js";
import { environment, MODEL, BINARY } from "../dist/isolation.js";
import { scopedAuth, redact, AUTH, PROVIDER } from "../dist/auth.js";
const root = await mkdtemp(join(tmpdir(), "agent-real-"));
try {
  const env = await environment(root);
  const local = new URL(
    "../../../infra/airflow/.local/secrets/agent_provider.json",
    import.meta.url,
  ).pathname;
  const source = await access(local).then(
    () => local,
    () => AUTH,
  );
  const secrets = await scopedAuth(root, source);
  const models = await capture(BINARY, ["models", PROVIDER], root, env, 30000);
  const available =
    models.exit_code === 0 && models.stdout.split("\n").includes(MODEL);
  console.log(JSON.stringify({ model: MODEL, available }));
  if (!available) throw new Error("Approved model unavailable");
  const hello = await capture(
    BINARY,
    ["run", "Say Hello.", "--model", MODEL, "--format", "json"],
    root,
    env,
    120000,
  );
  console.log(
    JSON.stringify({
      exit_code: hello.exit_code,
      error_class: hello.error_class,
      events: redact(hello.stdout, secrets),
      stderr: redact(hello.stderr, secrets),
    }),
  );
  if (hello.exit_code !== 0 || !hello.stdout.includes('"type":"text"'))
    throw new Error("Real Hello failed");
} finally {
  await rm(root, { recursive: true, force: true });
}
