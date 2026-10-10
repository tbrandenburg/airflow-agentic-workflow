import { copyFile } from "node:fs/promises";
await copyFile(
  new URL(
    "../../agent-contracts/schemas/v1/role-response.schema.json",
    import.meta.url,
  ),
  new URL("../dist/role-response.schema.json", import.meta.url),
);
