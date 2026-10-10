"""Track owned Linux descendants by PID and birth time, including new sessions."""

import os
import signal
from pathlib import Path


def processes() -> dict[int, tuple[int, str, str]]:
    result = {}
    for path in Path("/proc").glob("[0-9]*/stat"):
        try:
            text = path.read_text()
        except (FileNotFoundError, ProcessLookupError):
            continue
        fields = text[text.rfind(")") + 2 :].split()
        result[int(path.parent.name)] = (int(fields[1]), fields[19], fields[0])
    return result


class ProcessTree:
    def __init__(self) -> None:
        self.owned: dict[int, str] = {}

    def track(self, parent: int) -> None:
        snapshot = processes()
        if parent in snapshot:
            if parent in self.owned and self.owned[parent] != snapshot[parent][1]:
                return
            self.owned.setdefault(parent, snapshot[parent][1])
        changed = True
        while changed:
            changed = False
            for pid, (ppid, birth, _state) in snapshot.items():
                ancestor = snapshot.get(ppid)
                if (
                    ancestor is not None
                    and self.owned.get(ppid) == ancestor[1]
                    and pid not in self.owned
                ):
                    self.owned[pid] = birth
                    changed = True

    def kill(self) -> None:
        snapshot = processes()
        for pid, birth in self.owned.items():
            entry = snapshot.get(pid)
            if entry is None or entry[1] != birth or entry[2] == "Z":
                continue
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                continue

    def live(self) -> list[int]:
        snapshot = processes()
        return [
            pid
            for pid, birth in self.owned.items()
            if pid in snapshot and snapshot[pid][1] == birth and snapshot[pid][2] != "Z"
        ]
