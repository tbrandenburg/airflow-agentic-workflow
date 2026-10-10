import { readFile, mkdir, writeFile, lstat } from "node:fs/promises";
import { join } from "node:path";

export const PROVIDER = "github-copilot";
export const MODEL = "github-copilot/gpt-6.1-sol";
export const AUTH = "/model-auth/provider.json";

export async function scopedAuth(
  root: string,
  source = AUTH,
): Promise<string[]> {
  const info = await lstat(source);
  if (!info.isFile() || info.isSymbolicLink() || info.size > 65536)
    throw new Error("Invalid model credential file");
  const parsed: unknown = JSON.parse(await readFile(source, "utf8"));
  if (
    typeof parsed !== "object" ||
    parsed === null ||
    Array.isArray(parsed) ||
    Object.keys(parsed).join() !== PROVIDER
  )
    throw new Error("Credential provider not approved");
  const credential = (parsed as Record<string, unknown>)[PROVIDER];
  if (
    typeof credential !== "object" ||
    credential === null ||
    Array.isArray(credential)
  )
    throw new Error("Invalid model credential");
  const fields = credential as Record<string, unknown>;
  if (
    fields.type !== "oauth" ||
    typeof fields.refresh !== "string" ||
    !fields.refresh ||
    typeof fields.access !== "string" ||
    !fields.access ||
    typeof fields.expires !== "number" ||
    Object.keys(fields).some(
      (name) => !["type", "refresh", "access", "expires"].includes(name),
    )
  )
    throw new Error("Invalid approved OAuth credential");
  const directory = join(root, "session", "data", "opencode");
  await mkdir(directory, { recursive: true, mode: 0o700 });
  await writeFile(join(directory, "auth.json"), JSON.stringify(parsed), {
    mode: 0o600,
    flag: "wx",
  });
  return [fields.refresh, fields.access];
}

export function redact(text: string, secrets: string[]): string {
  return secrets
    .reduce((value, secret) => value.split(secret).join("[REDACTED]"), text)
    .replace(
      /\b(?:gh[opsu]_[A-Za-z0-9_]+|github_pat_[A-Za-z0-9_]+)\b/g,
      "[REDACTED]",
    )
    .replace(/Bearer\s+[^\s"\\]+/gi, "Bearer [REDACTED]")
    .replace(
      /("(?:access_token|refresh_token|authorization|apiKey)"\s*:\s*")[^"]*(")/gi,
      "$1[REDACTED]$2",
    );
}
