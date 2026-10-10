import assert from "node:assert/strict";
import { test } from "node:test";
import { spawn } from "node:child_process";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { transcript } from "../dist/response.js";

test("transcript preserves text and reports malformed event lines", () => {
  assert.deepEqual(
    transcript('{bad\n{"type":"text","part":{"text":"evidence"}}\n'),
    {
      text: "evidence",
      diagnostics: ["Event line 1: Malformed event JSON"],
    },
  );
});

test("Python AgentProcess cancellation stops detached Node child and descendant", async () => {
  const root = await mkdtemp(join(tmpdir(), "agent-cancel-"));
  const workspace = new URL("../../../", import.meta.url).pathname;
  const transport = new URL("../dist/transport.js", import.meta.url).href;
  const python = join(workspace, ".venv/bin/python");
  const wrapper = join(root, "wrapper.mjs");
  const child = join(root, "child.mjs");
  let process;
  try {
    await writeFile(
      child,
      `import {spawn} from 'node:child_process'; const child=spawn(process.execPath,['-e','setInterval(()=>{},1000)'],{stdio:'inherit'}); console.log(JSON.stringify({parent:process.pid,descendant:child.pid})); setInterval(()=>{},1000);`,
    );
    await writeFile(
      wrapper,
      `import {capture} from ${JSON.stringify(transport)}; import {writeFile} from 'node:fs/promises'; const result=await capture(process.execPath,[${JSON.stringify(child)}],${JSON.stringify(root)},{PATH:'/usr/bin:/bin'},60000); await writeFile(${JSON.stringify(join(root, "result.json"))},JSON.stringify(result));`,
    );
    const code =
      "import json,sys,threading; from pathlib import Path; from airflow_core.agent_process import AgentProcess; p=AgentProcess('planner','github-copilot/gpt-6.1-sol'); timer=threading.Timer(2,p.cancel); timer.start(); result=p.invoke([sys.argv[1],sys.argv[2]],Path(sys.argv[3]),20); timer.join(); print(json.dumps({'cancelled':result.cancelled,'exit':result.exit_code}))";
    process = spawn(
      python,
      ["-c", code, globalThis.process.execPath, wrapper, root],
      {
        cwd: workspace,
        env: {
          PATH: "/usr/bin:/bin",
          PYTHONPATH: join(workspace, "airflow-core"),
        },
        stdio: ["ignore", "pipe", "pipe"],
      },
    );
    const errors = [];
    process.stderr.on("data", (data) => errors.push(data.toString()));
    const status = await new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        process.kill("SIGKILL");
        reject(new Error("Cancellation test deadline"));
      }, 25000);
      process.once("error", reject);
      process.once("close", (code) => {
        clearTimeout(timer);
        resolve(code);
      });
    });
    assert.equal(status, 0, errors.join(""));
    const result = JSON.parse(
      await readFile(join(root, "result.json"), "utf8"),
    );
    assert.equal(result.error_class, "cancelled");
    const pids = JSON.parse(result.stdout.trim());
    for (const pid of [pids.parent, pids.descendant]) {
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
