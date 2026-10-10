"""Synthetic project's fixed credential-pattern check; never loads real credentials."""

import re
import sys
from pathlib import Path

PATTERN = re.compile(
    r"(?:-----BEGIN .*PRIVATE KEY-----|gh[pousr]_[A-Za-z0-9]{20,}|"
    r"AKIA[A-Z0-9]{16}|(?i:password|api_key|token)\s*=\s*['\"][^'\"]{8,})"
)
if PATTERN.search(Path(sys.argv[1]).read_text()):
    print("Credential pattern detected in proposed source")
    sys.exit(1)
print("No credential patterns detected")
