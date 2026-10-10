import { createServer } from "node:http";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { capture } from "../dist/transport.js";
import { environment, BINARY } from "../dist/isolation.js";
// Historical credential-free reproduction; never load the authorized provider secret here.
const MODEL = "opencode/big-pickle";

const root = await mkdtemp(join(tmpdir(), "agent-probe-"));
const server = createServer((request, response) => {
  let body = "";
  request.on("data", (chunk) => {
    body += chunk;
  });
  request.on("end", () => {
    const parsed = JSON.parse(body);
    const headers = Object.fromEntries(
      [
        "user-agent",
        "x-opencode-client",
        "x-opencode-session",
        "x-opencode-request",
      ].map((name) => [name, request.headers[name]]),
    );
    console.log(
      JSON.stringify({
        probe: "local-wire-inspection",
        path: request.url,
        headers,
        model: parsed.model,
        tool_count: parsed.tools?.length ?? 0,
      }),
    );
    response.writeHead(400, { "content-type": "application/json" });
    response.end(
      JSON.stringify({
        error: {
          message: "Local diagnostic endpoint; no inference",
          type: "diagnostic",
        },
      }),
    );
  });
});
try {
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  if (!address || typeof address === "string")
    throw new Error("No local listener");
  const env = await environment(root);
  const config = JSON.parse(env.OPENCODE_CONFIG_CONTENT);
  config.enabled_providers = ["opencode"];
  config.model = MODEL;
  config.small_model = MODEL;
  config.provider = {
    opencode: { options: { baseURL: `http://127.0.0.1:${address.port}/v1` } },
  };
  await capture(
    BINARY,
    ["run", "Say Hello.", "--model", MODEL, "--format", "json"],
    root,
    { ...env, OPENCODE_CONFIG_CONTENT: JSON.stringify(config) },
    60000,
  );
  await new Promise((resolve, reject) =>
    server.close((error) => (error ? reject(error) : resolve())),
  );
  // Documented defaults: no custom agent, implicit cli identity, default built-in plugins.
  const minimalRoot = join(root, "documented-defaults");
  const minimal = await environment(minimalRoot);
  delete minimal.OPENCODE_CLIENT;
  delete minimal.OPENCODE_PURE;
  delete minimal.OPENCODE_DISABLE_DEFAULT_PLUGINS;
  delete config.provider;
  delete config.agent;
  delete config.default_agent;
  minimal.OPENCODE_CONFIG_CONTENT = JSON.stringify(config);
  const result = await capture(
    BINARY,
    ["run", "Say Hello.", "--model", MODEL, "--format", "json"],
    minimalRoot,
    minimal,
    120000,
  );
  console.log(
    JSON.stringify({ probe: "documented-defaults-tools-denied", ...result }),
  );
} finally {
  if (server.listening) await new Promise((resolve) => server.close(resolve));
  await rm(root, { recursive: true, force: true });
}
