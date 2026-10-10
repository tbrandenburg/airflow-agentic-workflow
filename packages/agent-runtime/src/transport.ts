import { spawn } from "node:child_process";
import { performance } from "node:perf_hooks";

export type ErrorClass =
  | "timeout"
  | "cancelled"
  | "oversized"
  | "transport"
  | "rate_limit"
  | "permanent"
  | "invalid_output";
export interface Capture {
  stdout: string;
  stderr: string;
  exit_code: number | null;
  elapsed_ms: number;
  error_class: ErrorClass | null;
}

function kill(pid: number | undefined): void {
  if (pid === undefined) return;
  try {
    process.kill(-pid, "SIGKILL");
  } catch (error: unknown) {
    if (
      !(error instanceof Error) ||
      !("code" in error) ||
      error.code !== "ESRCH"
    )
      throw error;
  }
}

export function capture(
  command: string,
  args: string[],
  cwd: string,
  env: NodeJS.ProcessEnv,
  timeout: number,
  limit = 1048576,
): Promise<Capture> {
  return new Promise((resolve) => {
    const started = performance.now();
    const child = spawn(command, args, {
      cwd,
      env,
      shell: false,
      detached: true,
      stdio: ["ignore", "pipe", "pipe"],
    });
    const buffers = { stdout: Buffer.alloc(0), stderr: Buffer.alloc(0) };
    let failure: ErrorClass | null = null;
    const cancel = () => {
      failure = "cancelled";
      kill(child.pid);
    };
    process.once("SIGTERM", cancel);
    process.once("SIGINT", cancel);
    const timer = setTimeout(() => {
      failure ??= "timeout";
      kill(child.pid);
    }, timeout);
    for (const stream of ["stdout", "stderr"] as const) {
      child[stream].on("data", (data: Buffer) => {
        const remaining = Math.max(0, limit - buffers[stream].length);
        buffers[stream] = Buffer.concat([
          buffers[stream],
          data.subarray(0, remaining),
        ]);
        if (data.length > remaining) {
          failure ??= "oversized";
          kill(child.pid);
        }
      });
    }
    child.once("error", (error: Error) => {
      failure ??= "transport";
      buffers.stderr = Buffer.from(error.message).subarray(0, limit);
    });
    // exit can precede pipe closure when a descendant inherits stdout. Keep the deadline active.
    child.once("close", (code) => {
      clearTimeout(timer);
      kill(child.pid);
      process.removeListener("SIGTERM", cancel);
      process.removeListener("SIGINT", cancel);
      resolve({
        stdout: buffers.stdout.toString(),
        stderr: buffers.stderr.toString(),
        exit_code: code,
        elapsed_ms: Math.round(performance.now() - started),
        error_class: failure,
      });
    });
  });
}
