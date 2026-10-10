"""Fixed script invocation with bounded capture, clean environment and group cancellation."""

import os
import selectors
import signal
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path

from airflow_core.artifacts import LIMIT, ContractError
from airflow_core.process_tree import ProcessTree


@dataclass(frozen=True)
class ProcessResult:
    exit_code: int
    stdout: bytes
    stderr: bytes
    timed_out: bool
    cancelled: bool


class LogLimitError(ContractError):
    def __init__(self, stdout: bytes, stderr: bytes) -> None:
        super().__init__("Subprocess log size limit exceeded")
        self.stdout = stdout
        self.stderr = stderr


def script_argv(script: Path, input_path: Path, output_path: Path) -> list[str]:
    return [
        "bash",
        str(script),
        "--input-manifest",
        str(input_path),
        "--output-manifest",
        str(output_path),
    ]


class ScriptProcess:
    def __init__(self) -> None:
        self.process: subprocess.Popen[bytes] | None = None
        self.cancelled = False
        self.result: ProcessResult | None = None
        self.argv: list[str] = []
        self.tree = ProcessTree()

    def kill(self) -> None:
        if self.process is None:
            return
        self.tree.track(self.process.pid)
        self.tree.kill()
        try:
            os.killpg(self.process.pid, signal.SIGKILL)
        except ProcessLookupError:
            # Only the exact process group created here is addressed.
            self.process.wait(timeout=5)

    def cancel(self) -> None:
        self.cancelled = True
        self.kill()

    def run(self, script: Path, root: Path, timeout: int) -> ProcessResult:
        return self.invoke(
            script_argv(script, root / "input.json", root / "output.json"), root, timeout
        )

    def diagnostics(self, root: Path) -> dict[str, bytes]:
        return {}

    def identity(self) -> str:
        return "script"

    def invoke(
        self,
        argv: list[str],
        root: Path,
        timeout: int,
        *,
        environment: dict[str, str] | None = None,
        limit: int = LIMIT,
    ) -> ProcessResult:
        env = {
            "PATH": "/usr/local/bin:/usr/bin:/bin",
            "HOME": str(root),
            "TMPDIR": str(root),
            "LANG": "C.UTF-8",
        }
        if environment is not None:
            env = environment
        self.argv = argv
        # prlimit execs in the owned session, avoiding preexec_fn in Celery threads.
        bounded = [
            "prlimit",
            "--cpu=120:120",
            "--fsize=8388608:8388608",
            "--nofile=1024:1024",
            "--core=0:0",
            "--",
            *argv,
        ]
        self.process = subprocess.Popen(
            bounded,
            cwd=root,
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
            close_fds=True,
        )
        assert self.process.stdout is not None and self.process.stderr is not None
        stdout = bytearray()
        stderr = bytearray()
        timed_out = False
        deadline = time.monotonic() + timeout
        with selectors.DefaultSelector() as selector:
            selector.register(self.process.stdout, selectors.EVENT_READ, stdout)
            selector.register(self.process.stderr, selectors.EVENT_READ, stderr)
            try:
                while selector.get_map():
                    self.tree.track(self.process.pid)
                    if time.monotonic() >= deadline:
                        timed_out = True
                        self.kill()
                    for key, _events in selector.select(timeout=0.1):
                        chunk = os.read(key.fd, 65536)
                        if not chunk:
                            selector.unregister(key.fileobj)
                            continue
                        buffer = stdout if key.fileobj == self.process.stdout else stderr
                        if len(buffer) + len(chunk) > limit:
                            raise LogLimitError(bytes(stdout), bytes(stderr))
                        buffer.extend(chunk)
                remaining = max(0.01, deadline - time.monotonic())
                try:
                    self.process.wait(timeout=remaining)
                except subprocess.TimeoutExpired:
                    timed_out = True
            finally:
                self.kill()
                self.process.stdout.close()
                self.process.stderr.close()
        code = self.process.wait(timeout=5)
        deadline = time.monotonic() + 5
        while self.tree.live() and time.monotonic() < deadline:
            self.tree.kill()
            time.sleep(0.05)
        if self.tree.live():
            raise ContractError("Owned subprocess descendants survived cleanup")
        self.process = None
        self.result = ProcessResult(code, bytes(stdout), bytes(stderr), timed_out, self.cancelled)
        return self.result
