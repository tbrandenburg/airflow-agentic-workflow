import { mkdir } from "node:fs/promises";
import { join, dirname } from "node:path";
import { MODEL, PROVIDER } from "./auth.js";
export { MODEL } from "./auth.js";

export const VERSION = "1.18.32";
export const BINARY = new URL("../node_modules/.bin/opencode", import.meta.url)
  .pathname;

export async function environment(root: string): Promise<NodeJS.ProcessEnv> {
  const home = join(root, "session");
  for (const directory of ["config", "data", "cache", "state"])
    await mkdir(join(home, directory), { recursive: true });
  const config = {
    $schema: "https://opencode.ai/config.json",
    autoupdate: false,
    share: "disabled",
    snapshot: false,
    plugin: [],
    mcp: {},
    enabled_providers: [PROVIDER],
    provider: { [PROVIDER]: { whitelist: [MODEL.split("/")[1]] } },
    model: MODEL,
    small_model: MODEL,
    default_agent: "response",
    formatter: false,
    lsp: false,
    permission: { "*": "deny" },
    agent: {
      response: {
        mode: "primary",
        steps: 1,
        permission: { "*": "deny" },
        prompt:
          "Return only the requested role JSON. Context is supplied in the user message. Do not use tools.",
      },
    },
  };
  return {
    PATH: `${dirname(process.execPath)}:/usr/local/bin:/usr/bin:/bin`,
    HOME: home,
    TMPDIR: root,
    LANG: "C.UTF-8",
    HTTPS_PROXY: "http://model-egress:8080",
    NO_PROXY: "localhost,127.0.0.1,::1",
    XDG_CONFIG_HOME: join(home, "config"),
    XDG_DATA_HOME: join(home, "data"),
    XDG_CACHE_HOME: join(home, "cache"),
    XDG_STATE_HOME: join(home, "state"),
    OPENCODE_CONFIG_DIR: join(home, "config"),
    OPENCODE_CONFIG_CONTENT: JSON.stringify(config),
    OPENCODE_DISABLE_PROJECT_CONFIG: "true",
    OPENCODE_DISABLE_AUTOUPDATE: "true",
    OPENCODE_PURE: "true",
    OPENCODE_CLIENT: "cli",
    OPENCODE_DISABLE_CLAUDE_CODE: "true",
    OPENCODE_DISABLE_EXTERNAL_SKILLS: "true",
    OPENCODE_DISABLE_MODELS_FETCH: "true",
    OPENCODE_DISABLE_LSP_DOWNLOAD: "true",
    OPENCODE_EXPERIMENTAL_DISABLE_FILEWATCHER: "true",
    OPENCODE_DISABLE_AUTOCOMPACT: "true",
  };
}
