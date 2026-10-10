import { readFileSync } from "node:fs";
import { Ajv2020 } from "ajv/dist/2020.js";
import type { ErrorClass } from "./transport.js";

export type Role = "planner" | "coder" | "reviewer";
export interface Response {
  role: Role;
  [key: string]: unknown;
}
export class ResponseError extends Error {
  constructor(
    public readonly error_class: ErrorClass,
    message: string,
  ) {
    super(message);
  }
}
export function transcript(events: string): {
  text: string;
  diagnostics: string[];
} {
  const diagnostics: string[] = [];
  const text = events
    .split("\n")
    .flatMap((line, index) => {
      if (!line.trim()) return [];
      try {
        const event = object(JSON.parse(line) as unknown);
        if (event.type !== "text") return [];
        const part = object(event.part);
        if (typeof part.text !== "string")
          throw new ResponseError("invalid_output", "Invalid text event");
        return [part.text];
      } catch (error: unknown) {
        diagnostics.push(
          `Event line ${index + 1}: ${error instanceof ResponseError ? error.message : "Malformed event JSON"}`,
        );
        return [];
      }
    })
    .join("");
  return { text, diagnostics };
}
const schema: unknown = JSON.parse(
  readFileSync(new URL("./role-response.schema.json", import.meta.url), "utf8"),
);
if (typeof schema !== "object" || schema === null)
  throw new Error("Missing role schema");
const validate = new Ajv2020({ strict: false, discriminator: false }).compile(
  schema,
);

function object(value: unknown): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value))
    throw new ResponseError("invalid_output", "Invalid event object");
  return value as Record<string, unknown>;
}

export function extract(
  events: string,
  role: Role,
): { text: string; response: Response } {
  let text = "";
  let finished = false;
  for (const line of events.split("\n").filter((item) => item.trim())) {
    let parsed: unknown;
    try {
      parsed = JSON.parse(line);
    } catch {
      throw new ResponseError("invalid_output", "Malformed event JSON");
    }
    const event = object(parsed);
    if (event.type === "error") {
      const error = object(event.error);
      const data =
        typeof error.data === "object" && error.data !== null
          ? object(error.data)
          : {};
      const status = data.statusCode;
      const retryable = status === 429 || data.isRetryable === true;
      throw new ResponseError(
        status === 429 ? "rate_limit" : retryable ? "transport" : "permanent",
        "OpenCode error event",
      );
    }
    if (event.type === "text") {
      const part = object(event.part);
      if (typeof part.text !== "string" || finished)
        throw new ResponseError("invalid_output", "Invalid text event");
      text += part.text;
    }
    if (event.type === "step_finish") {
      const part = object(event.part);
      if (part.reason === "stop") finished = true;
      if (["length", "error", "cancelled"].includes(String(part.reason)))
        throw new ResponseError("invalid_output", "Incomplete response");
    }
    if (event.type === "tool_use")
      throw new ResponseError("invalid_output", "Tool invocation rejected");
  }
  if (!finished || !text)
    throw new ResponseError(
      "invalid_output",
      "Missing complete assistant response",
    );
  let parsed: unknown;
  try {
    parsed = JSON.parse(
      text.trim().replace(/^```json\s*\n([\s\S]*)\n```$/, "$1"),
    );
  } catch {
    throw new ResponseError("invalid_output", "Malformed application JSON");
  }
  if (!validate(parsed))
    throw new ResponseError("invalid_output", "Role schema rejected");
  const response = object(parsed) as Response;
  if (response.role !== role)
    throw new ResponseError("invalid_output", "Role mismatch");
  return { text, response };
}
