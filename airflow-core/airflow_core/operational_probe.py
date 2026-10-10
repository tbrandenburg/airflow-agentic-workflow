"""Fixed synthetic process-lifecycle proof, executed inside each deployed worker."""

import threading
from pathlib import Path

from airflow_core.artifacts import ArtifactStore, canonical
from airflow_core.process import ScriptProcess
from airflow_core.workspace import workspace


def lifecycle(prefix: str) -> dict[str, object]:
    evidence: dict[str, object] = {}
    for mode in ("timeout", "cancelled"):
        with workspace() as root:
            script = root / "probe.py"
            script.write_text(
                "import subprocess,time\n"
                "p=subprocess.Popen(['sleep','30'],start_new_session=True)\n"
                "open('pid','w').write(str(p.pid))\n"
                "print('started',flush=True)\ntime.sleep(30)\n"
            )
            process = ScriptProcess()
            timer = threading.Timer(1, process.cancel)
            if mode == "cancelled":
                timer.start()
            try:
                result = process.invoke(["python", str(script)], root, 2)
            finally:
                if mode == "cancelled":
                    timer.join(timeout=5)
            assert result.cancelled if mode == "cancelled" else result.timed_out
            assert process.tree.live() == []
            pid = int((root / "pid").read_text())
            stat = Path(f"/proc/{pid}/stat")
            assert not stat.exists() or stat.read_text().split(") ")[1].startswith("Z")
            evidence[mode] = {
                "exit_code": result.exit_code,
                "timed_out": result.timed_out,
                "cancelled": result.cancelled,
                "live_pids": process.tree.live(),
            }
        assert not root.exists()
    ref = ArtifactStore.deployment().put(
        f"{prefix}/lifecycle.json", canonical(evidence), "application/json"
    )
    return {"evidence": evidence, "uri": ref.uri}
