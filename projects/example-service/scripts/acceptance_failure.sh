#!/usr/bin/env bash
set -euo pipefail
# Fixed deployment-only failure; caller input never selects this executable.
printf 'P6_FIXED_BASH_FAILURE exit=42\n' >&2
exit 42
