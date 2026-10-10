import { spawn } from "node:child_process";
import type { Role } from "./response.js";

export const MODES = [
  "success",
  "transient",
  "permanent",
  "timeout",
  "malformed",
  "schema-invalid",
  "missing-patch",
  "oversized",
  "slow",
  "unsafe-patch",
] as const;
export type Mode = (typeof MODES)[number];
export function fixture(role: Role, mode: Mode, attempt: number): void {
  if (mode === "slow") {
    setTimeout(() => fixture(role, "success", attempt), 12000);
    return;
  }
  if (mode === "timeout") {
    const child = spawn(process.execPath, ["-e", "setInterval(()=>{},1000)"], {
      stdio: "inherit",
    });
    process.stderr.write(`descendant=${child.pid}\n`);
    process.exit(0);
  }
  if (mode === "oversized") {
    process.stdout.write("x".repeat(2097152));
    return;
  }
  if (mode === "permanent" || (mode === "transient" && attempt === 1)) {
    process.stderr.write("fixture transport diagnostic\n");
    process.stdout.write(
      JSON.stringify({
        type: "error",
        error: {
          name: "APIError",
          data: {
            statusCode: mode === "permanent" ? 401 : 429,
            isRetryable: mode === "transient",
          },
        },
      }) + "\n",
    );
    return;
  }
  const response =
    role === "planner"
      ? {
          schema_version: "1.0",
          role,
          plan: "Add validation to service.py",
          suggested_paths: ["service.py"],
          assumptions: ["Independent source context"],
          validation_focus: ["Invalid inputs"],
        }
      : role === "coder"
        ? {
            schema_version: "1.0",
            role,
            summary: "Proposed documentation",
            unified_diff:
              mode === "unsafe-patch"
                ? "diff --git a/.git/config b/.git/config\n--- a/.git/config\n+++ b/.git/config\n@@ -1 +1 @@\n-old\n+unsafe\n"
                : 'diff --git a/service.py b/service.py\n--- a/service.py\n+++ b/service.py\n@@ -1,2 +1,2 @@\n def health() -> str:\n-    return "ok"\n+    return "healthy"\n',
          }
        : {
            schema_version: "1.0",
            role,
            findings: [],
            recommendations: [],
            verdict: "approved",
          };
  const text =
    mode === "malformed"
      ? "{"
      : mode === "schema-invalid"
        ? JSON.stringify({ role })
        : mode === "missing-patch"
          ? JSON.stringify({
              schema_version: "1.0",
              role: "coder",
              summary: "No patch",
            })
          : JSON.stringify(response);
  process.stderr.write("fixture diagnostic\n");
  for (const event of [
    { type: "step_start", part: {} },
    { type: "text", part: { text } },
    { type: "step_finish", part: { reason: "stop" } },
  ])
    process.stdout.write(JSON.stringify(event) + "\n");
}
